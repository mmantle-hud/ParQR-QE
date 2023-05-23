package kggenerator

import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

/*
Computes which GADM region YAGO points are within
 */

object PointInPolygon {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Point in Polygon Tests")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
      .getOrCreate()


    SedonaSQLRegistrator.registerAll(spark)
    System.setProperty("sedona.global.charset", "utf8")

    import spark.implicits._

    val pointsInputDir = args(0)
    val regionsInputDir = args(1)
    val matchesDir = args(2)
    val outputDir = args(3)


    val regionDF = spark.read.parquet(regionsInputDir)
    regionDF.createOrReplaceTempView("regiontable")
    regionDF.cache()
    regionDF.show()

    val pointDF = spark.read.parquet(pointsInputDir)
    pointDF.cache()
    pointDF.show()
    println("There are "+pointDF.count+" points")

    val matches = spark.read.parquet(matchesDir+"layer0").union(spark.read.parquet(matchesDir+"layer1"))
    matches.cache()
    matches.show(false)

    //now remove the point for which we have matches
    val cleanedPoints = pointDF.as("p")
      .join(matches.as("m"),$"p.subject" === $"m.subject", "left_anti")
    cleanedPoints.createOrReplaceTempView("pointtable")


    //Compute containment for YAGO points
    val resultsDF = spark.sql(
      "SELECT regiontable.region_id, regiontable.name, pointtable.subject FROM regiontable, pointtable " +
      "WHERE ST_Contains(regiontable.geometry,pointtable.geometry)"
    )
    resultsDF.cache()
    resultsDF.show(50, false)
    println("There are "+resultsDF.count+" computed relations")

    //get points that haven't been placed
    val unplacedPoints = pointDF.as("p").join(resultsDF.as("r"),$"p.subject"===$"r.subject","left_anti")

    //save results
   resultsDF.write.mode("overwrite").parquet(outputDir)
   unplacedPoints.write.mode("overwrite").parquet(pointsInputDir+"/unplaced")

  }
}
