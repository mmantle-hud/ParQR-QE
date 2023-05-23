package kggenerator

import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.asc

/*
Using GADM polygons compute EC relations between regions
 */
object ComputeEC {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Compute EC relations between regions in a layer")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
      .getOrCreate()

    SedonaSQLRegistrator.registerAll(spark)

    val regions = spark.read.parquet(args(0))
    //specifies a window to filter by, allows us to limit the size of the join
    val coords = args(1).split("#").mkString(",")
    val outputDir = args(2)


    regions.createOrReplaceTempView("regions")
    regions.cache()
    regions.show()

    //select regions that intersect the window
    val filteredRegions = spark.sql(
        "SELECT * FROM regions WHERE ST_Intersects (ST_PolygonFromEnvelope("+coords+"), regions.geometry)"
    )


    filteredRegions.createOrReplaceTempView("spatialdf")
    filteredRegions.cache()

    filteredRegions.createOrReplaceTempView("lhsRegions")
    filteredRegions.createOrReplaceTempView("rhsRegions")

    //compute adjacency relations for selected regions
    val resultsDf = spark.sql(
      "SELECT lhsRegions.region_id as region_id1, lhsRegions.name as name1, rhsRegions.region_id as region_id2, rhsRegions.name as name2 FROM lhsRegions, rhsRegions " +
        "WHERE ST_Touches(lhsRegions.geometry, rhsRegions.geometry) AND lhsRegions.region_id != rhsRegions.region_id"
    ).distinct()

    resultsDf.sort(asc("region_id1")).show()

    //save relations
    resultsDf.write.mode("overwrite").parquet(outputDir)

  }
}
