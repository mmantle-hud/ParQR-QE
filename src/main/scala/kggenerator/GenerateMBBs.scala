package kggenerator

import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{collect_list, first}

/*
Computes the MBB for GADM multi-polygon regions.
 */

object GenerateMBBs {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Generate MBBs")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
      .getOrCreate()

    SedonaSQLRegistrator.registerAll(spark)
    import spark.implicits._

    val regions = spark.read.parquet(args(0))
    val outputDir = args(1)
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
        | SELECT region_id, name, ST_Envelope(ST_Collect(geoms)) as mbb
        | FROM groupedregiontable
        """.stripMargin)

    groupedRegions.createOrReplaceTempView("groupedregiontable")
    groupedRegions.cache()
    groupedRegions.show(false)
    groupedRegions.write.mode("overwrite").parquet(outputDir)

  }
}
