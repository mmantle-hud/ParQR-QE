package kggenerator.delete

import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_list, first}

object GenerateCentroids {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      //.master("local[4]") // Delete this if run in cluster mode
      .appName("Generate Centroids") // Change this to a proper name
      .config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
      .getOrCreate() // org.apache.sedona.core.serde.SedonaKryoRegistrator


    SedonaSQLRegistrator.registerAll(spark)
    import spark.implicits._
    /*
    //region data
    |region_id|geometry|name|
    //point data
    |subject|geometry|
    //rdf:type info
    |subject|object|
    //rdfs:label
    |subject|object|
     */

    // var regions = spark.read.parquet("./data/output/regions/parquet/NE/layer0")
    var regions = spark.read.parquet(args(0))
    val outputDir = args(1)
    regions.createOrReplaceTempView("regions")
    regions = spark.sql(
      """
        | SELECT ST_GeomFromWKT(regions.geometry) AS geometry, region_id, name
        | FROM regions
        """.stripMargin)
    regions.createOrReplaceTempView("regions")
    regions.cache()
    regions.show()

    var groupedRegions = regions
      .groupBy($"region_id")
      .agg(
        first($"name").as("name"),
        collect_list($"geometry").as("geoms")
      )

    groupedRegions.createOrReplaceTempView("groupedregiontable")
    groupedRegions = spark.sql(
      """
        | SELECT region_id, name, ST_Centroid(ST_Collect(geoms)) as centroid
        | FROM groupedregiontable
        """.stripMargin)

    groupedRegions.createOrReplaceTempView("groupedregiontable")
    groupedRegions.cache()
    groupedRegions.show(false)

    var stringDF = spark.sql(
      """
        |SELECT ST_AsText (groupedregiontable.centroid) as centroid, region_id, name
        |FROM groupedregiontable
  """.stripMargin)


    stringDF.show()
    stringDF.write.parquet(outputDir)

  }
}
