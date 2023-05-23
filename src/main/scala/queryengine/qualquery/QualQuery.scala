package queryengine.qualquery

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import queryengine.helpers.TimeHelper
import queryengine.reasoner.{Calculus, PreProcessFncs, QualitativeSpatialReasoner}
import queryengine.sparql.{ConvertTriplesToSQL, QueryParser}

import scala.collection.mutable.HashMap

/*
Runs GeoSPARQL containment and adjacency queries qualitatively
 */

object QualQuery {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Query Engine")
      .config("spark.master", "local[4]")
      .getOrCreate()


    val rcc8NetworkDir = args(0)
    val dictionaryDir = args(1)
    val calculusDir = args(2)
    val predicates = spark.sparkContext.textFile(args(3)).collect()
    val yagoDir = args(4)
    val queriesPath = args(5)
    val partitions = args(6).toInt

    //load the RCC-8 calculus
    val calculusRDD = spark.sparkContext.textFile(calculusDir).collect
    val calculus = new Calculus(calculusRDD(0),calculusRDD(1),calculusRDD.drop(2))

    //load the RCC8 network
    val inputTriples = spark.sparkContext.textFile(rcc8NetworkDir)
    //convert the input lines into tuples of the form (Int,BitSet,Int,Int)
    val inputRels = PreProcessFncs.convertInput(inputTriples)
    //get transpose of input network
    val inverseRels = PreProcessFncs.getTranspose(inputRels, calculus)
    //combine transpose with initial input
    val allInputRels = inputRels.union(inverseRels).distinct()
    //get the dictionary (needed for look-ups between the query and the RCC-8 network)
    val dictionary = spark.read.parquet(dictionaryDir).cache()

    //convert BitSet into Int for input dataset
    val setToIntlookUp = calculus.setToIntLookUp
    val universalRel = calculus.universalRel
    val allTriples = allInputRels
      .map(x=>(x._1,setToIntlookUp(x._2),x._3,x._4))
      .repartition(partitions)
      .cache()

    //pre-compute results of operations (speeds-up reasoning)
    //get a list of input relations and generate possible results of composition  based on this
    val possibleRels=PreProcessFncs.generatePossibleRelations(allInputRels, calculus)
    //using this list of relations pre-compute the composition table
    val compTbl = PreProcessFncs.preComputeCompTbl(possibleRels, calculus)
    //the same but pre-compute the intersection table
    val intersectTbl = PreProcessFncs.preComputeIntersectTbl(possibleRels, calculus)
    //now generate a size of relation look-up table
    val relSizeTbl = PreProcessFncs.preComputeSizeTbl(possibleRels, calculus)

    println("Loaded an RCC8 network of size:"+allTriples.count())
    println("It has this many partitions:"+allTriples.getNumPartitions)

    //group triples by head node, this will form the broadcastQCN
    val keyedTriples = allTriples
      .keyBy(_._1)
      .groupByKey
      .mapValues(_.toList)
      .collectAsMap

    val keyedTriplesB = spark.sparkContext.broadcast(keyedTriples)
    val compTblB = spark.sparkContext.broadcast(compTbl)
    val intersectTblB = spark.sparkContext.broadcast(intersectTbl)
    val relSizeTblB = spark.sparkContext.broadcast(relSizeTbl)

    println("Broadcast triple var size:"+keyedTriplesB.value.size)
    println("Broadcast Composition table var size:"+compTblB.value.size)
    println("Broadcast Intersection table var size:"+intersectTblB.value.size)
    println("Broadcast Relation size table var size:"+relSizeTblB.value.size)

    //Now load the YAGO-VP tables
    predicates.foreach(p=>{
      val parts = p.split("/")
      val df = spark.read.parquet(yagoDir+parts(0)+"/"+parts(1))
      df.createOrReplaceTempView(parts(1))
    })

    //load the queries
    val queries = spark
      .sparkContext
      .textFile(queriesPath)
      .collect()
      .mkString("\n")
      .split("-----")

    var qCount = 1

    println(queries.size)

    //iterate over the queries
    queries.foreach(queryStr=> {
      println(queryStr)
      TimeHelper.time {
          val queryParser = new QueryParser(queryStr)

        //result vars are the columns we want to select
        // i.e. the variables in 'SELECT ?state ?actor ...
        val resultVars = queryParser.getResultVars()

        //do the non spatial part of the query
        val nonSpatialTriples = queryParser.getNonSpatialTriples()
        //convert non-spatial query triples to SQL
        val queriesArr = ConvertTriplesToSQL.convert(nonSpatialTriples)

        var results = Map[String, DataFrame]()

        //execute each non-spatial SQL statement and cache the results
        queriesArr.foreach(query=>{
          val sqlDF = spark.sql(query).distinct()
            .cache()
          val columns = sqlDF.columns.toSeq
          columns.foreach(c => results += (c -> sqlDF))
        })

//        println("These are the results of the non-spatial query")
//        for ((k,v) <- results) {
//          println("key: "+k+":"+results(k).count)
//          results(k).show(false)
//        }

        //now the spatial part of the query
        val spatialTriple = queryParser.getSpatialTriple()


        //first get hold of the right-hand part of the query triple
        //this tells us if it is a join
        val queryObjects = if(spatialTriple._3.indexOf("?") > -1){
          //It's a join e.g. ?birthplace geo:sfWithin ?state
          val rhsSpatialObjectVar = spatialTriple._3.substring(1)
          //we need to get the query objects from the non-spatial part of the query
          results(rhsSpatialObjectVar)
            .select(col(rhsSpatialObjectVar).as("rhsSpatialObjectVar"))
            .distinct.collect.map(x=>x(0).toString())
        }else{
          // It's not a join e.g. ?birthPlace geo:sfWithin yago:Catalonia
          // we are going to reason for a single query object
          Array(spatialTriple._3)
        }

        // now get the spatial relation
        val rel = spatialTriple._2
        // map to a set
        val setRel = if(rel == "sfTouches" || rel == "sf-touches") {
          Set(2)
        }else{
          Set(6,7)
        }

        val qualResults = executeQualQuery(
          spark,
          allTriples,
          keyedTriplesB,
          dictionary,
          compTblB,
          intersectTblB,
          relSizeTblB,
          calculus,
          queryObjects,
          setRel,
          partitions
        )

        println("Qual results")
        qualResults.show(false)


        val join = if(spatialTriple._3.indexOf("?") > -1){
          //it's a join
          val rhsSpatialObjectVar = spatialTriple._3.substring(1)
          val lhsSpatialObjectVar = spatialTriple._1.substring(1)
          qualResults
            .join(results(rhsSpatialObjectVar),col("parent") === col(rhsSpatialObjectVar))
            .drop(col("parent"))
            .join(results(lhsSpatialObjectVar),col("child") === col(lhsSpatialObjectVar))
            .drop("child")
        }else{
          //not a join
          val lhsSpatialObjectVar = spatialTriple._1.substring(1)
          qualResults
            .join(results(lhsSpatialObjectVar),col("child") === col(lhsSpatialObjectVar))
            .drop("child")
            .drop("parent")
        }

        join.createOrReplaceTempView("finalresults")
        val resultVarsStr = resultVars.reduce((a,b)=>{
          a+","+b
        })

        val finalResult = spark.sql("SELECT "+resultVarsStr+" FROM finalresults").cache()
        println("Final results:"+finalResult.count)
        finalResult.show(false)

        finalResult.unpersist()

      }

    })

  }

  def executeQualQuery(spark:SparkSession,
                       allTriples:RDD[(Int,Int,Int,Int)],
                       keyedTriplesB:Broadcast[scala.collection.Map[Int,List[(Int,Int,Int,Int)]]],
                       dictionary:DataFrame,
                       compTblB:Broadcast[HashMap[Int, HashMap[Int,Int]]],
                       intersectTblB:Broadcast[HashMap[Int,HashMap[Int,Int]]],
                       relSizeTblB:Broadcast[HashMap[Int,Int]],
                       calculus: Calculus,
                       queryObjects:Array[String],
                       setRel:Set[Int],
                       partitions:Int
                      ):DataFrame={
    import spark.implicits._

    //look-up ids from the dictionary
    val queryObjectsAsInts = dictionary.filter($"id".isin(queryObjects:_*))
      .select($"encodedId")
      .collect().map(x=>x(0).asInstanceOf[Long].toInt)

    //Now reason for the query objects
    val reasoningResults = QualitativeSpatialReasoner.reason(
      spark,
      allTriples,
      keyedTriplesB,
      dictionary,
      compTblB,
      intersectTblB,
      relSizeTblB,
      calculus,
      queryObjectsAsInts,
      partitions
    )
    //filter results by type of relation
    val intToSetLookUp = calculus.intToSetLookUp
    val filteredResults = reasoningResults
      .map(x=> {
        (x._1,intToSetLookUp(x._2),x._3)
      })
      .filter(x=> x._2.intersect(setRel).size > 0 && x._2.size <= setRel.size)
      .map(x=>(x._1,x._3))

    //convert the RDD into a dataframe
    val resultsDF = spark.createDataFrame(filteredResults).toDF("parent","child")

    //use the dictionary to substitute back in the string representations
    val qualResults = resultsDF
      .join(dictionary,$"parent"===$"encodedId")
      .select($"id".as("parent"),$"child")
      .join(dictionary,$"child"===$"encodedId")
      .select($"parent",$"id".as("child"))

    //return results
    qualResults

  }

}
