package spatialpartition.partitioning

import org.apache.spark.sql.SparkSession

object GenerateGeomIndex {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Run Queries")
      .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val geomDir = args(0)
    val pointsDir = args(1)
    val partitions = args(2).toInt
    val outputDir = args(3)

    val geoms = spark.read.parquet(geomDir+"level0")
  .union(spark.read.parquet(geomDir+"level1"))
  .withColumn("geomtype",lit("polygon"))
  .select($"id",$"gid",$"fullid",$"coords",$"count",$"geomtype")


    val convertGeom = udf((lat:Double,lng:Double)=>{Array(Array(Array(lat,lng)))})


    val points = spark.read.parquet(pointsDir)
      .withColumn("coords",convertGeom($"lat",$"lng"))
      .withColumn("geomtype",lit("point"))
      .withColumn("id",$"subject")
      .withColumn("gid",$"subject")
      .withColumn("fullid",$"subject")
      .withColumn("count",lit(1))
      .select($"id",$"gid",$"fullid",$"coords",$"count",$"geomtype")



    val all = geoms.union(points).cache()

    all.write
      .mode("overwrite")
      .bucketBy(partitions, "fullid")
      .sortBy("fullid")
      .option("path", outputDir)
      .saveAsTable("geoms")

  }
}
