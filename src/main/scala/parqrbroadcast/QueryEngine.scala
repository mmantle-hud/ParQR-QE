package parqrbroadcast

import helpers.TimeHelper.time
import org.apache.spark.sql.SparkSession

object QueryEngine {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Run Queries")
      .getOrCreate()




    val parquetDir = args(0)
    val configDir = args(1)
    val rcc8NetworkDir = args(2)
    val dictionaryDir = args(3)
    val predicatesFile = args(4)
    val queriesFile = args(5)
    val partitions = args(6).toInt

    val sc = spark.sparkContext


    //generate the calculus
    val config = sc.textFile(configDir).collect
    val calculus=new Calculus(config(0),config(1),config.drop(2))

    //load the RCC8 network
    var inputTriples = sc.textFile(rcc8NetworkDir)

    //convert the input lines into tuples of the form (Int,BitSet,Int,Int)
    val inputRels = PreProcessFncs.convertInput(inputTriples,calculus)
    //get transpose of input network
    val inverseRels = PreProcessFncs.getTranspose(inputRels,calculus)
    //combine transpose with initial input
    val allInputRels = inputRels.union(inverseRels)



    val dictionary = spark.read.parquet(dictionaryDir).cache()

    println("Items in the dictionary:"+dictionary.count)



    //convert BitSet into Int for input dataset
    val setToIntlookUp = calculus.setToIntLookUp
    val universalRel = calculus.universalRel
    val allTriples = allInputRels.map(x=>(x._1,setToIntlookUp(x._2),x._3,x._4)).cache()

    //get a list of input relations and generate possible results of composition  based on this
    val possibleRels=PreProcessFncs.generatePossibleRelations(allInputRels,calculus)

    //using this list of relations pre-compute the composition table
    val compTbl = PreProcessFncs.preComputeCompTbl(possibleRels,calculus)

    //the same but pre-compute the intersection table
    val intersectTbl = PreProcessFncs.preComputeIntersectTbl(possibleRels, calculus)

    //now generate a size of relation look-up table
    val relSizeTbl = PreProcessFncs.preComputeSizeTbl(possibleRels, calculus)

    val keyedTriples = allTriples
      .keyBy(_._1)
      .groupByKey
      .mapValues(_.toList)
      .collectAsMap


    println("Loaded an RCC8 network of size:"+allTriples.count())
    println("It has this many partitions:"+allTriples.getNumPartitions)

    val keyedTriplesB = sc.broadcast(keyedTriples)
    val compTblB = sc.broadcast(compTbl)
    val intersectTblB = sc.broadcast(intersectTbl)
    val relSizeTblB = sc.broadcast(relSizeTbl)

    println("Broadcast triple var size:"+keyedTriplesB.value.size)
    println("Broadcast Composition table var size:"+compTblB.value.size)
    println("Broadcast Intersection table var size:"+intersectTblB.value.size)
    println("Broadcast Relation size table var size:"+relSizeTblB.value.size)


    val predicates = spark
      .sparkContext
      .textFile(predicatesFile)
          .collect()

    predicates.foreach(p=>{
      val parts = p.split("/")
      val df = spark.read.parquet(parquetDir+parts(0)+"/"+parts(1))
      df.createOrReplaceTempView(parts(1))
      //df.cache()
      //println(parts(1)+":"+df.count())
    })

    val queries = spark
      .sparkContext
      .textFile(queriesFile)
      .collect()
      .mkString("\n")
      .split("-----")


var qCount =1
    queries.foreach(query=> {
      time{
        val resultVars = queryengine.QueryParsingSQL.getResultVars(query)
        val triples = queryengine.QueryParsingSQL.getTriples(query)


        val nonSpatialTriples = queryengine.QueryParsingSQL.getNonSpatialTriples(triples)
        var results = queryengine.QueryParsingSQL.getNonSpatialResults(spark,nonSpatialTriples)

        val spatialTriples = queryengine.QueryParsingSQL.getSpatialTriples(triples)

        val nonJoinSpatialTriples = spatialTriples.filter(triple => !(triple._3.indexOf("?") > -1))

        if(nonJoinSpatialTriples.size > 0) {
          nonJoinSpatialTriples.foreach(triple=>{
            results = QueryParsingQualSpatial.runQuery(
              spark,
              triple,
              results,
              calculus,
              allTriples,
              keyedTriplesB,
              dictionary,
              compTblB,
              intersectTblB,
              relSizeTblB,
              partitions
            )
          })
        }

        val joinSpatialTriples = spatialTriples.filter(triple=>triple._3.indexOf("?") > -1)

        if(joinSpatialTriples.size > 0) {
          joinSpatialTriples.foreach(triple=>{
            results = QueryParsingQualSpatial.runQuery(
              spark,
              triple,
              results,
              calculus,
              allTriples,
              keyedTriplesB,
              dictionary,
              compTblB,
              intersectTblB,
              relSizeTblB,
              partitions
            )
          })
        }

        println("Final Results:")
        val resultsDF = results(resultVars(0))
        resultsDF.createOrReplaceTempView("results")

        val resultVarsStr = resultVars.reduce((a,b)=>{
          a+","+b
        })

        val finalResult = spark.sql("SELECT "+resultVarsStr+" FROM results").cache()
        println("Final results:"+finalResult.count)
        finalResult.show(50,false)
        finalResult.unpersist()
        qCount = qCount+1
      }
    })

  }
}
