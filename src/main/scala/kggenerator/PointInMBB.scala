package kggenerator

import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

/*
Computes point in polygon YAGO points and MBBs.
MBBs are needed as centroids are used to represent large regions e.g. countries.
In the case of multi-polygons regions, the centroid can lie outside the region polygons e.g. Malaysia
 */


object PointInMBB {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Point in MBB")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
      .getOrCreate()


    SedonaSQLRegistrator.registerAll(spark)
    System.setProperty("sedona.global.charset", "utf8")

    val pointsInputDir = args(0)
    val mBBInputDir = args(1)
    val outputDir = args(2)

    val mBBDF = spark.read.parquet(mBBInputDir)
    mBBDF.createOrReplaceTempView("mbbtable")
    mBBDF.cache()
    mBBDF.show()

    val pointDF = spark.read.parquet(pointsInputDir)
    pointDF.createOrReplaceTempView("pointtable")
    pointDF.cache()
    pointDF.show()


    val resultsDF = spark.sql(
      "SELECT mbbtable.region_id, mbbtable.name, pointtable.subject FROM mbbtable, pointtable " +
      "WHERE ST_Contains(mbbtable.mbb,pointtable.geometry)"
    )

   resultsDF.write.mode("overwrite").parquet(outputDir)

  }
}
