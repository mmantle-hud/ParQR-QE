package spatialpartition.partitioning

import com.google.common.geometry.S2CellId
import helpers.TimeHelper
import org.apache.spark.sql.SparkSession
import queryengine.QueryParsingSQL
import org.apache.spark.sql.functions._

object QuanQueryEngine {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Run Queries")
      .getOrCreate()

    import spark.implicits._



    val parquetDir = args(0)
    val spatialIndexDir = args(1)
    val geomsDir = args(2)
    val predicatesFile = args(3)
    val queriesFile = args(4)
    val partitions = args(5).toInt
    val parentLevel = args(6).toInt


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


    val geoms = spark.read.parquet(geomsDir)

    def getParentToken = udf((token:String)=>{
      S2CellId.fromToken(token).parent(parentLevel).id
    })
    val spatialIndex = spark.read.parquet(spatialIndexDir)
      .select($"token",$"id",$"fullid",$"count")
      .withColumn("cell",getParentToken($"token"))
      .cache()

    spatialIndex.show(false)

    println("We have "+spatialIndex.count+" items in the  spatial index")


var qCount = 1
    queries.foreach(query=> {
      TimeHelper.time {
        val resultVars = QueryParsingSQL.getResultVars(query)
        val triples = QueryParsingSQL.getTriples(query)


        val nonSpatialTriples = QueryParsingSQL.getNonSpatialTriples(triples)
        var results = QueryParsingSQL.getNonSpatialResults(spark, nonSpatialTriples)

        val spatialTriples = QueryParsingSQL.getSpatialTriples(triples)

        val nonJoinSpatialTriples = spatialTriples.filter(triple => !(triple._3.indexOf("?") > -1))

        if(nonJoinSpatialTriples.size > 0) {
          nonJoinSpatialTriples.foreach(triple=>{
            results = QueryParsingQuanSpatial.runQuery(spark, triple, results, spatialIndex,geoms)
          })
        }

        val joinSpatialTriples = spatialTriples.filter(triple=>triple._3.indexOf("?") > -1)

        if(joinSpatialTriples.size > 0) {
          joinSpatialTriples.foreach(triple=>{
            results = QueryParsingQuanSpatial.runQuery(spark, triple, results, spatialIndex,geoms)
          })
        }



        val resultsDF = results(resultVars(0))
        resultsDF.createOrReplaceTempView("results")

        val resultVarsStr = resultVars.reduce((a, b) => {
          a + "," + b
        })
        val finalResult = spark.sql("SELECT " + resultVarsStr + " FROM results").cache()
        println("Final results:" + finalResult.count)
        finalResult.show(false)
        finalResult.unpersist()
        qCount = qCount+1
      }
    })
  }
}
