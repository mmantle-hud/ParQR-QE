package generateindex

import org.apache.spark.sql.SparkSession

object GenerateGeomsIndex {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Generate Geoms Index")
      //        .config("spark.master", "local[4]")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val geomDir = args(0)
    val geomIndexDir = args(1)
    val pointsDir = args(2)
    val pointsIndexDir = args(3)
    val partitions = args(4).toInt
    val outputDir = args(5)

//    val geomDir = "gs://mmantle/part3/temp/matching/polygons/"
//    val geomIndexDir = "gs://mmantle/part3/temp/index2/"
//    val pointsDir = "gs://mmantle/part3/temp/matching/points"
//    val pointsIndexDir = "gs://mmantle/part3/temp/matching/index/points"
//    val partitions = 16
//    val outputDir = "gs://mmantle/part3/dataset/geoms"

    val geoms = spark.read.parquet(geomDir+"level0").union(spark.read.parquet(geomDir+"level1"))

    val geomIndex = spark.read.parquet(geomIndexDir+"level0")
      .union(spark.read.parquet(geomIndexDir+"level1"))
      .select($"token",$"fullid".as("fullid_i"))

    //$"token",$"id",$"fullid",$"coords",$"count",$"geomtype"
    val geomsAndTokens = geomIndex.join(geoms,$"fullid_i"===$"fullid")
      .withColumn("geomtype",lit("polygon"))
      .select($"token",$"id",$"gid",$"fullid",$"coords",$"count",$"geomtype")

    val convertGeom = udf((lat:Double,lng:Double)=>{Array(Array(Array(lat,lng)))})


    val points = spark.read.parquet(pointsDir)
      .withColumn("coords",convertGeom($"lat",$"lng"))
      .withColumn("geomtype",lit("point"))
      .withColumn("id",$"subject")
      .withColumn("gid",$"subject")
      .withColumn("fullid",$"subject")
      .withColumn("count",lit(1))

    val pointsIndex = spark.read.parquet(pointsIndexDir).select($"token",$"id".as("id_i"))

    val pointsAndTokens = points
      .join(pointsIndex,$"id" === $"id_i")
      .select($"token",$"id",$"gid",$"fullid",$"coords",$"count",$"geomtype")

    val all = geomsAndTokens.union(pointsAndTokens).repartition(partitions).cache()

    println(all.count)

    all.write.format("parquet").mode("overwrite").save(outputDir)
  }
}
