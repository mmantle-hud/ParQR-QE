package generateindex

import org.apache.spark.sql.SparkSession

object GenerateBasicIndex {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Generate basic index")
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
//    val geomIndexDir = "gs://mmantle/part3/dataset/region-indexes/basic-index4/"
//    val pointsIndexDir = "gs://mmantle/part3/dataset/point-indexes/index4/points"
//    val partitions = 16
//    val outputDir = "gs://mmantle/part3/dataset/combined-index/index4"

    val geoms = spark.read.parquet(geomDir+"level0").union(spark.read.parquet(geomDir+"level1")).select($"gid",$"fullid",$"id",$"count")

    val geomIndex = spark.read.parquet(geomIndexDir+"level0").union(spark.read.parquet(geomIndexDir+"level1")).select($"token",$"fullid".as("fullid_i"))

    val geomsAndTokens = geomIndex.join(geoms,$"fullid_i"===$"fullid").select($"token",$"id",$"gid",$"fullid",$"count")

    val points = spark.read.parquet(pointsDir)

    val pointsIndex = spark.read.parquet(pointsIndexDir)

    val pointsAndTokens = points.as("p").join(pointsIndex.as("i"),$"p.subject"===$"i.id")
      .select(
        $"i.id".as("id"),
        $"i.token".as("token"),
        $"p.lat".as("lat"),
        $"p.lng".as("lng")
      )
      .withColumn("gid",$"id")
      .withColumn("fullid",$"id")
      .withColumn("count",lit(1))
      .select($"token",$"id",$"gid",$"fullid",$"count")

    val all = geomsAndTokens.union(pointsAndTokens).repartition(partitions)

    all.write.format("parquet").mode("overwrite").save(outputDir)
  }
}
