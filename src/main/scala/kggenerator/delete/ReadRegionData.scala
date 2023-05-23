package kggenerator.delete

import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

object ReadRegionData {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .master("local[4]") // Delete this if run in cluster mode
      .appName("Read Region Data") // Change this to a proper name
      .config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
      .getOrCreate() // org.apache.sedona.core.serde.SedonaKryoRegistrator


    SedonaSQLRegistrator.registerAll(spark)
    System.setProperty("sedona.global.charset", "utf8")

    val inputDir = args(0)
    val subj = args(1)
    val partitions = args(2).toInt

    val subjSeq = subj.split(",")
    subjSeq.foreach(println)
    // val inputDir = "./data/output/regions/parquet/GADM/level1"
    // val subj = "BEL.1_1"

    var regions = spark.read.parquet(inputDir).repartition(partitions)
    regions.cache()
    regions.createOrReplaceTempView("regionstable")

    regions.show()

    regions = spark.sql(
      """
        | SELECT ST_GeomFromWKT(regionstable.geometry) AS geometry, region_id, name
        | FROM regionstable
        """.stripMargin)
    regions.createOrReplaceTempView("regionstable")

    regions.show()
    import spark.implicits._

    val subjectDF = regions
      .where($"region_id".isin(subjSeq: _*))

    /*
      spark.sql("SELECT * FROM regionstable WHERE regionstable.region_id = '"+subj+"'")
*/
    subjectDF.cache()
    subjectDF.createOrReplaceTempView("subjecttable")
    subjectDF.show()

    val resultsDf = spark.sql(
      "SELECT subjecttable.name, subjecttable.region_id, regionstable.name, regionstable.region_id FROM subjecttable, regionstable " +
        "WHERE ST_Touches(subjecttable.geometry, regionstable.geometry) AND subjecttable.region_id <> regionstable.region_id")
    resultsDf.createOrReplaceTempView("resultsdf")
    resultsDf.distinct.show(50)


  }
}
