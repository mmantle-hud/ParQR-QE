package kggenerator.delete

import org.apache.sedona.core.enums.IndexType
import org.apache.sedona.sql.utils.{Adapter, SedonaSQLRegistrator}
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

object ReadRegionDataIndex {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      //.master("local[4]") // Delete this if run in cluster mode
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

    var regionDF = spark.read.parquet(inputDir).repartition(partitions)
    regionDF.cache()
    regionDF.createOrReplaceTempView("regiontable")

    regionDF.show()

    regionDF = spark.sql(
      """
        | SELECT ST_GeomFromWKT(regiontable.geometry) AS geometry, region_id, name
        | FROM regiontable
        """.stripMargin)
    regionDF.createOrReplaceTempView("regiontable")

    regionDF.show()


    import spark.implicits._

    var subjectDF = regionDF
      .where($"region_id".isin(subjSeq: _*))

    /*
      spark.sql("SELECT * FROM regiontable WHERE regionstable.region_id = '"+subj+"'")
*/
    subjectDF.cache()
    subjectDF.createOrReplaceTempView("subjecttable")
    subjectDF.show()

    //now convert the DFs to RDDs so we can index them
    val regionRDD = Adapter.toSpatialRdd(regionDF, geometryFieldName = "geometry", fieldNames = Seq("region_id, name"))
    val subjectRDD = Adapter.toSpatialRdd(subjectDF, geometryFieldName = "geometry", fieldNames = Seq("region_id, name"))

    //apply the indexing
    //regionRDD.spatialPartitioning(GridType.KDBTREE)
    //subjectRDD.spatialPartitioning(regionRDD.getPartitioner)

    regionRDD.buildIndex(IndexType.QUADTREE, false)

    //convert back into DFs so that we can use the touches predicate
    regionDF = Adapter.toDf(regionRDD, Seq("region_id", "name"), spark)
    subjectDF = Adapter.toDf(subjectRDD, Seq("region_id", "name"), spark)
    regionDF.createOrReplaceTempView("regiontable")
    subjectDF.createOrReplaceTempView("subjecttable")

    regionDF.cache()
    subjectDF.cache()

    println("There are " + regionDF.count + " regions")
    println("There are " + subjectDF.count + " subject")


    val resultsDf = spark.sql(
      "SELECT subjecttable.name, subjecttable.region_id, regiontable.name, regiontable.region_id FROM subjecttable, regiontable " +
        "WHERE ST_Touches(subjecttable.geometry, regiontable.geometry) AND subjecttable.region_id <> regiontable.region_id")
    resultsDf.createOrReplaceTempView("resultsdf")
    resultsDf.distinct.show(50)


  }
}
