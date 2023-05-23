package kggenerator

import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

/*
Loads schema:geo triples and converts into Sedona points.
 */

object ConvertPointsToGeoSpark {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("Convert YAGO point to spatial RDD")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
      .getOrCreate()

    SedonaSQLRegistrator.registerAll(spark)

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val yagoDir = args(0)
    val outputDir = args(1)

    val geoDF = spark.read.parquet(yagoDir)
    geoDF.show(false)


    val extractLat = udf((coord:String)=>{
      val lat = coord.substring(5,coord.indexOf(',')).toDouble
      lat
    })
    val extractLng = udf((coord:String)=>{
      val lng = coord.substring(coord.indexOf(',')+1,coord.length-1).toDouble
      lng
    })

    val latLngData = geoDF
      .withColumn("lat",extractLat($"object"))
      .withColumn("lng",extractLng($"object"))
      .select($"subject",$"lat",$"lng")
      .cache()

    latLngData.createOrReplaceTempView("points")

    // convert the coordinates to GeoSpark Points
    var spatialDf = spark.sql(
      """
        |SELECT ST_Point(points.lng, points.lat) AS geometry, subject
        |FROM points
    """.stripMargin)

    spatialDf.show(false)
    spatialDf.createOrReplaceTempView("points")

    spatialDf.write.mode("overwrite").parquet(outputDir)


  }
}