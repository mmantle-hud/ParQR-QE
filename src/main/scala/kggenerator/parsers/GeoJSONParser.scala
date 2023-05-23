package kggenerator.parsers

import org.apache.sedona.core.formatMapper.GeoJsonReader
import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

/*
Loads a GeoJSON file converts it to a Spark dataframe
The geometry is stored as a Sedona geometry
*/

object GeoJSONParser {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Parse GeoJSON")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
      .getOrCreate()

    SedonaSQLRegistrator.registerAll(spark)


    val inputDir = args(0)
    val outputDir = args(1)
    val nameCol = args(2)
    val regionIdCol = args(3)


    //load GeoJSON data
    val allowTopologyInvalidGeometries = false // Optional
    val skipSyntaxInvalidGeometries = false // Optional
    val spatialRDD = GeoJsonReader
      .readToGeometryRDD(
        spark.sparkContext,
        inputDir,
        allowTopologyInvalidGeometries,
        skipSyntaxInvalidGeometries
      )

    //convert to dataframe
    var spatialDF = Adapter.toDf(spatialRDD, spark)
    spatialDF.createOrReplaceTempView("gadm")

    //select relevant columns
    spatialDF = spark.sql("SELECT gadm.geometry as geometry, gadm."+regionIdCol+" as region_id, " +
      "gadm."+nameCol+" as name FROM gadm")

    spatialDF.show()
    spatialDF.write.mode("overwrite").parquet(outputDir)

  }
}
