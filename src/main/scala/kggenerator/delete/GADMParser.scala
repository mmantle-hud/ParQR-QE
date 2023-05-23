package kggenerator.delete

import org.apache.sedona.core.formatMapper.GeoJsonReader
import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

/*

 */
object GADMParser {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[4]") // Delete this if run in cluster mode
      .appName("Parse GADM") // Change this to a proper name
      // Enable Sedona custom Kryo serializer
      .config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
      .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)
      .getOrCreate() // org.apache.sedona.core.serde.SedonaKryoRegistrator


    SedonaSQLRegistrator.registerAll(spark)

    //    val inputDir = "./data/geoms/gadm/gadm36_BEL_1.json"
    val inputDir = "./example/src/gadm/layer0.json"
    val outputDir = "./data/output/regions/GADM/level1"

    spark.read.json(inputDir).printSchema()
    System.setProperty("sedona.global.charset", "utf8")

    //load GeoJSON data
    val allowTopologyInvalidGeometries = false // Optional
    val skipSyntaxInvalidGeometries = true // Optional
    val spatialRDD = GeoJsonReader
      .readToGeometryRDD(
        spark.sparkContext,
        inputDir,
        allowTopologyInvalidGeometries,
        skipSyntaxInvalidGeometries
      )

    var spatialDF = Adapter.toDf(spatialRDD, spark)
    spatialDF.createOrReplaceTempView("geoyago")

    spatialDF.show()

    /*
    // flip the coordinates from (lon,lat) to (lat,lon) so that we can apply ST_Transform
    spatialDF = spark.sql(
      """
        |SELECT ST_FlipCoordinates(geoyago.geometry) as geometry, geoyago.adm1_code as region_id, geoyago.NAME as name
        |FROM geoyago
    """.stripMargin)
    spatialDF.show(false)
    spatialDF.createOrReplaceTempView("geoyago")


    // convert the coordinate reference system
    spatialDF = spark.sql(
      """
        |SELECT ST_Transform(geoyago.geometry, 'epsg:4326','epsg:3857') as geometry, geoyago.region_id, geoyago.name
        |FROM geoyago
    """.stripMargin)
    spatialDF.show(false)
    spatialDF.createOrReplaceTempView("geoyago")

    // flip the coordinates back so that we have the X coordinate first
    spatialDF = spark.sql(
      """
        |SELECT ST_FlipCoordinates(geoyago.geometry) as geometry, geoyago.region_id, geoyago.name
        |FROM geoyago
    """.stripMargin)
    spatialDF.show(false)
    spatialDF.createOrReplaceTempView("geoyago")

    spatialDF.show()

    //now convert to a spatialRDD so that we can store
    val spatialRDDForSaving = Adapter.toSpatialRdd(spatialDF, geometryFieldName="geometry", fieldNames = Seq("region_id, name"))
    spatialRDD.rawSpatialRDD.saveAsObjectFile(outputDir)
*/


    /*
    val noOfCountries = spatialDF.select(
      col("region_id"),
      col("name")
    )
      .distinct()
      .count()

    println("There are " + noOfCountries + " countries")
    */
  }
}
