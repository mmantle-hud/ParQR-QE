package spatialpartition.partitioning

import com.google.common.geometry.S2CellId
import org.apache.spark.sql.SparkSession

object GeneratePartitionIndex {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Generate partition index")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._


    val geomDir = args(0)
    val geomIndexDir = args(1)
    val pointsDir = args(2)
    val pointsIndexDir = args(3)
    val partitions = args(4).toInt
    val outputDir = args(5)


    val geoms = spark.read.parquet(geomDir+"level0").union(spark.read.parquet(geomDir+"level1"))

    val geomIndex = spark.read.parquet(geomIndexDir+"level0").union(spark.read.parquet(geomIndexDir+"level1")).select($"token",$"fullid".as("fullid_i"))

    val geomsAndTokens = geomIndex.join(geoms,$"fullid_i"===$"fullid").withColumn("geomtype",lit("polygon")).select($"token",$"id",$"gid",$"fullid",$"coords",$"count",$"geomtype")

    val convertGeom = udf((lat:Double,lng:Double)=>{Array(Array(Array(lat,lng)))})


    val points = spark.read.parquet(pointsDir)

    val pointsIndex = spark.read.parquet(pointsIndexDir).select($"token",$"id".as("id_i"))

    val pointsAndTokens = pointsIndex.join(points,$"id_i" === $"subject").withColumn("coords",convertGeom($"lat",$"lng")).withColumn("geomtype",lit("point")).withColumn("id",$"subject").withColumn("gid",$"subject").withColumn("fullid",$"subject").withColumn("count",lit(1)).select($"token",$"id",$"gid",$"fullid",$"coords",$"count",$"geomtype")


    val convertToken = udf((token:String)=>{
        S2CellId.fromToken(token).id
    })
    val all = geomsAndTokens
      .union(pointsAndTokens)
      .withColumn("cell",convertToken($"token"))
      .repartition(partitions)
      .sort($"cell").cache()

    println(all.count)

    all.write.mode("overwrite").format("parquet").partitionBy("cell").save(outputDir)
    }
}
