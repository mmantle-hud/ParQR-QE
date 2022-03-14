package windowquanfirst

import com.google.common.geometry.S2CellId
import helpers.TimeHelper
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import parqrbroadcast.{Calculus, ParQR, PreProcessFncs}
import queryengine.QueryParsingSQL

object QueryEngine {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Run Window Queries")
      .getOrCreate()

    import spark.implicits._

    val parquetDir = args(0)

    //quan
    val spatialIndexDir = args(1)
    val geomsDir = args(2)

    //qual
    val configDir = args(3)
    val rcc8NetworkDir = args(4)
    val dictionaryDir = args(5)

    //query
    val predicatesFile = args(6)
    val queriesFile = args(7)
    val partitions = args(8).toInt
    val parentLevel = args(9).toInt



    //start of qual preparation
    val sc = spark.sparkContext
    //generate the calculus
    val config = sc.textFile(configDir).collect
    val calculus=new Calculus(config(0),config(1),config.drop(2))
    //load the RCC8 network
    val inputTriples = sc.textFile(rcc8NetworkDir)
    //convert the input lines into tuples of the form (Int,BitSet,Int,Int)
    val inputRels = PreProcessFncs.convertInput(inputTriples,calculus)
    //get transpose of input network
    val inverseRels = PreProcessFncs.getTranspose(inputRels,calculus)
    //combine transpose with initial input
    val allInputRels = inputRels.union(inverseRels)
    //load the dictionary
    val dictionary = spark.read.parquet(dictionaryDir).cache()
    println("Items in the dictionary:"+dictionary.count)
    //convert BitSet into Int for input dataset
    val setToIntlookUp = calculus.setToIntLookUp
    val universalRel = calculus.universalRel
    val allTriples = allInputRels.map(x=>(x._1,setToIntlookUp(x._2),x._3,x._4))
    //get a list of input relations and generate possible results of composition  based on this
    val possibleRels=PreProcessFncs.generatePossibleRelations(allInputRels,calculus)
    //using this list of relations pre-compute the composition table
    val compTbl = PreProcessFncs.preComputeCompTbl(possibleRels,calculus)
    //the same but pre-compute the intersection table
    val intersectTbl = PreProcessFncs.preComputeIntersectTbl(possibleRels, calculus)
    //now generate a size of relation look-up table
    val relSizeTbl = PreProcessFncs.preComputeSizeTbl(possibleRels, calculus)

    val keyedTriples = allTriples.keyBy(_._1).groupByKey.mapValues(_.toList).collectAsMap
    println("Loaded an RCC8 network of size:"+allTriples.count())
    println("It has this many partitions:"+allTriples.getNumPartitions)
    val keyedTriplesB = sc.broadcast(keyedTriples)
    val compTblB = sc.broadcast(compTbl)
    val intersectTblB = sc.broadcast(intersectTbl)
    val relSizeTblB = sc.broadcast(relSizeTbl)
    println("Broadcast var size:"+keyedTriplesB.value.size)
    //end of qual preparation

    //start of quan preparation
    val geoms = spark.read.parquet(geomsDir)

    def getParentToken = udf((token:String)=>{
      //S2CellId.fromToken(token).parent(parentLevel).toToken
      S2CellId.fromToken(token).parent(parentLevel).id
    })
    val spatialIndex = spark.read.parquet(spatialIndexDir)
      .select($"token",$"id",$"fullid",$"count")
      .withColumn("cell",getParentToken($"token"))
      .cache()

    val predicates = spark
      .sparkContext
      .textFile(predicatesFile)
      .collect()

    predicates.foreach(p=>{
      val parts = p.split("/")
      val df = spark.read.parquet(parquetDir+parts(0)+"/"+parts(1))
      df.createOrReplaceTempView(parts(1))
    })

    val queries = spark
      .sparkContext
      .textFile(queriesFile)
      .collect()
      .mkString("\n")
      .split("-----")


var qCount = 1
    queries.foreach(query=> {
      TimeHelper.time {
        val resultVars = QueryParsingSQL.getResultVars(query)
        val triples = QueryParsingSQL.getTriples(query)


        val nonSpatialTriples = QueryParsingSQL.getNonSpatialTriples(triples)
        var results = QueryParsingSQL.getNonSpatialResults(spark, nonSpatialTriples)

        val triple = QueryParsingSQL.getSpatialTriples(triples).head
        println(triple)

        //using the query window, get candidate quan objects
        val initResult = QueryParsingQuanSpatial.getCandidates(spark, triple, results, spatialIndex,geoms)

        val initCandidates = initResult._2
        val cells = initResult._1

        //now check the geoms
        val quanResults = QueryParsingQuanSpatial.filterCandidates(
          spark,
          triple,
          results,
          initCandidates,
          cells,
          geoms
        ).cache()

        println("quan results:"+quanResults.count)


        val candidates = quanResults.rdd.map(x=>x(0).toString).collect()

        val qualResults = ParQR.query(
          spark,
          candidates,
          Set(6,7),
          calculus,
          allTriples,
          keyedTriplesB,
          dictionary,
          compTblB,
          intersectTblB,
          relSizeTblB,
          partitions
        ).cache()

        println("qual results:"+qualResults.count)
        qualResults.show(false)


        val finalSpatialResults = quanResults
          .union(
            qualResults
              .withColumn("id",$"child")
              .drop("parent","child")
          )

        println("Final spatial results (quan and qual):"+finalSpatialResults.count)
        finalSpatialResults.show(false)

        val queryVar = triple._1.substring(1)
        val join = finalSpatialResults
          .join(results(queryVar),$"id"===col(queryVar))
          .drop("id")
          .distinct()
          .cache()

        join.columns.toSeq.foreach(col=>{
          results(col) = join
        })

        val resultsDF = results(resultVars(0))
        resultsDF.createOrReplaceTempView("results")

        val resultVarsStr = resultVars.reduce((a, b) => {
          a + "," + b
        })
        val finalResult = spark.sql("SELECT " + resultVarsStr + " FROM results")
        println("Final results:" + finalResult.count)
        finalResult.show(false)

      }
    })
  }
}
