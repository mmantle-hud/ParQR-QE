package kggenerator.delete

import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

object LinkYAGO {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      //.master("local[4]")
      .appName("Match YAGO Points to GADM")
      .config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
      .getOrCreate() // org.apache.sedona.core.serde.SedonaKryoRegistrator


    SedonaSQLRegistrator.registerAll(spark)
    import spark.implicits._


    //var regions = spark.read.parquet("./data/output/regions/parquet/NE/layer0")
    //var regions = spark.read.parquet("./data/output/regions/parquet/GADM/level1-fra-bel")
    var regions = spark.read.parquet(args(0))
    regions.createOrReplaceTempView("regions")

    //var centroids = spark.read.parquet("./data/output/centroids/level0")
    //var centroids = spark.read.parquet("./data/output/centroids/level1-fra-bel-che")
    var mbbs = spark.read.parquet(args(1))
    mbbs.createOrReplaceTempView("mbbs")

    //var points = getPoints(spark)
    var points = spark.read.parquet(args(2))
    points.createOrReplaceTempView("pointtable")

    //val labels = getLabels(spark)
    var labels = spark.read.parquet(args(3)).filter($"object".substr(-3, 3) === "@en").distinct
    labels.createOrReplaceTempView("labels")

    //val types = getTypes(spark)
    val types = spark.read.parquet(args(4))
    types.createOrReplaceTempView("types")

    //val dissolution = getDissolution(spark)
    val dissolution = spark.read.parquet(args(5))
    dissolution.createOrReplaceTempView("dissolution")

    //val userType = "<http://schema.org/AdministrativeArea>" //"http://schema.org/Country"
    val userType = "<" + args(6) + ">" //"http://schema.org/Country"

    //val outputDir = "./data/output/matches/layer0"
    val outputDir = args(7) //"./data/output/matches/layer0"

    points = spark.sql(
      """
        | SELECT ST_GeomFromWKT(pointtable.geometry) AS geometry, subject
        | FROM pointtable
        """.stripMargin)
    points.createOrReplaceTempView("pointtable")
    points.cache()
    points.show(false)
    println("There are " + points.count + " total points")


    // join points to types to get points of correct type only
    val pointsByType = points.as("p")
      .join(types.as("t"), $"p.subject" === $"t.subject")
      .where($"t.object" === userType)
      .select(
        $"p.subject".as("subject"),
        $"p.geometry".as("geometry")
      )
    println("There are " + pointsByType.count + " of type " + userType)
    pointsByType.show(false)


    //left outer join with dissolution data to get current regions only
    var currentPoints = pointsByType.as("c")
      .join(dissolution.as("d"), $"c.subject" === $"d.subject", "left_outer")
      .filter($"d.subject".isNull)
      .select(
        $"c.subject".as("subject"),
        $"c.geometry".as("geometry")
      )

    println("Current YAGO points of type" + userType)
    currentPoints.createOrReplaceTempView("pointtable")
    currentPoints.show(false)
    println("There are " + currentPoints.count + " after removal of dissolution")

    mbbs.createOrReplaceTempView("mbbs")
    mbbs = spark.sql(
      """
        | SELECT ST_GeomFromWKT(mbbs.geometry) AS geometry, region_id, name
        | FROM mbbs
        """.stripMargin)
    mbbs.createOrReplaceTempView("mbbs")
    mbbs.cache()
    mbbs.show()

    var containsDF = spark.sql(
      "SELECT mbbs.region_id, mbbs.name, pointtable.subject FROM centroids, pointtable " +
        "WHERE ST_Contains(centroids.geometry, pointtable.geometry)"
    )

    containsDF.cache()
    containsDF.show(false)

    println("There are " + containsDF.count + " computed relations i.e. points within mbbs")

    //at this point we assume all points have been placed

  }
}
