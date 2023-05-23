package kggenerator.delete

import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

object ReadRegionDataTwoTables {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      //.master("local[4]") // Delete this if run in cluster mode
      .appName("Read Region Data") // Change this to a proper name
      .config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
      .getOrCreate() // org.apache.sedona.core.serde.SedonaKryoRegistrator


    SedonaSQLRegistrator.registerAll(spark)
    System.setProperty("sedona.global.charset", "utf8")

    val table1Dir = args(0)
    val table2Dir = args(1)
    val subj1 = args(2)
    val subj2 = args(3)
    val partitions = args(4).toInt

    val subj1Seq = subj1.split(",")
    subj1Seq.foreach(println)
    // val inputDir = "./data/output/regions/parquet/GADM/level1"
    // val subj = "BEL.1_1"

    //these are the regions that will be filtered to leave only the subjects
    var subjectRegions = spark.read.parquet(table1Dir).repartition(partitions)
    subjectRegions.cache()
    subjectRegions.createOrReplaceTempView("subjectregions")
    subjectRegions = spark.sql(
      """
        | SELECT ST_GeomFromWKT(subjectregions.geometry) AS geometry, region_id, name
        | FROM subjectregions
        """.stripMargin)
    subjectRegions.createOrReplaceTempView("subjectregions")
    subjectRegions.show()

    import spark.implicits._
    val subjectDF = subjectRegions
      .where($"region_id".isin(subj1Seq: _*))
    subjectDF.cache()
    subjectDF.createOrReplaceTempView("subjecttable")
    subjectDF.show()


    //these are the regions that will be searched through
    var regions = spark.read.parquet(table2Dir).repartition(partitions)
    regions.cache()
    regions.createOrReplaceTempView("regionstable")

    regions = spark.sql(
      """
        | SELECT ST_GeomFromWKT(regionstable.geometry) AS geometry, region_id, name
        | FROM regionstable
        """.stripMargin)

    regions = regions
      .where($"region_id" contains subj2)

    regions.createOrReplaceTempView("regionstable")

    regions.show()


    //now run the query
    val resultsDf = spark.sql(
      "SELECT subjecttable.name, subjecttable.region_id, regionstable.name, regionstable.region_id FROM subjecttable, regionstable " +
        "WHERE ST_Touches(subjecttable.geometry, regionstable.geometry) AND subjecttable.region_id <> regionstable.region_id")
    resultsDf.createOrReplaceTempView("resultsdf")
    resultsDf.distinct.show(50)


  }
}
