package kggenerator.matching

import info.debatty.java.stringsimilarity.JaroWinkler
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/*
Matches GADM layer 0 regions to YAGO points
 */

object LinkYAGOtoGADM0 {


  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Match YAGO Points to GADM Layer 0")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
      .getOrCreate()


    SedonaSQLRegistrator.registerAll(spark)
    import spark.implicits._

    val polygons = spark.read.parquet(args(0)).select($"region_id", $"name").distinct()

    val labels = spark.read.parquet(args(1)).filter($"object".substr(-3, 3) === "@en").distinct
    labels.createOrReplaceTempView("labels")

    val types = spark.read.parquet(args(2))
    types.createOrReplaceTempView("types")

    val dissolution = spark.read.parquet(args(3))
    dissolution.createOrReplaceTempView("dissolution")

    val userType = "<" + args(4) + ">"

    val pointInMBB = spark.read.parquet(args(5)).select($"region_id", $"subject")

    val outputDir = args(6) //"./data/output/matches/layer0"

    // join pointsInPolygons to types to get points of correct type only
    val pointsByType = pointInMBB.as("p")
      .join(types.as("t"), $"p.subject" === $"t.subject")
      .where($"t.object" === userType)
      .select(
        $"p.subject".as("subject"),
        $"p.region_id".as("region_id")
      )

    //left outer join with dissolution data to get current regions only
    val currentPoints = pointsByType.as("c")
      .join(dissolution.as("d"), $"c.subject" === $"d.subject", "left_outer")
      .filter($"d.subject".isNull)
      .select(
        $"c.subject".as("subject"),
        $"c.region_id".as("region_id")
      )

    //join with GADM region info so we can compare labels to names
    val pointsAndRegions = currentPoints.as("points")
      .join(polygons.as("polygons"), $"points.region_id" === $"polygons.region_id")
      .select(
        $"points.subject".as("subject"),
        $"points.region_id".as("region_id"),
        $"polygons.name".as("name")
      ).distinct()

    pointsAndRegions.show(false)

    //now join spatial results to the labels and do text similarity
    val getShortLabel = udf((label: String) => {
      val shortLabel = label.substring(1, label.indexOf("@") - 1)
      val normalLabel = if (shortLabel.lastIndexOf(",") > -1)
        shortLabel.substring(0, shortLabel.lastIndexOf(","))
      else {
        shortLabel
      }
      normalLabel
    })

    def findNameInLabel = udf((subject: String, name: String) => {
      val parts = name.split(" ")
      var found = true
      parts.foreach(part => {
        if (!(subject contains part)) found = false
      })
      found
    })

    def computeSimilarity = udf((subject: String, name: String) => {
      val jw = new JaroWinkler()
      jw.similarity(name, subject)
    })

    //now join spatial results to the labels and do text similarity
    val resultsAndLabels = pointsAndRegions.as("r")
      .join(labels.as("l"), $"r.subject" === $"l.subject")
      .select(
        $"r.subject".as("subject"),
        $"r.region_id".as("region_id"),
        $"r.name".as("name"),
        $"l.object".as("label"),
      )
      .withColumn("label", getShortLabel($"label"))
      .withColumn("similarity", computeSimilarity($"label", $"name"))
      .withColumn("match", findNameInLabel($"label", $"name"))

    //convert to RDD, this makes the aggregation easier
    val countriesRDD = resultsAndLabels.rdd.map(row => {
      (row(1).toString, (row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString().toDouble, row(5).toString))
    })

    val chosenRDD = countriesRDD.reduceByKey((a, b) => {
      val chosen = if (a._6 == "true" && b._6 == "false") {
        a
      } else if (a._6 == "false" && b._6 == "true") {
        b
      } else if (a._5 > b._5) {
        a
      } else {
        b
      }
      chosen
    })
    val reducedRDD = chosenRDD.map(x => x._2).map(x => Row(x._1, x._2, x._3, x._4, x._5.toString, x._6))

    //convert back to a df
    val schemaString = "subject region_id name label similarity match"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val matchesDF = spark.createDataFrame(reducedRDD, schema)
    matchesDF.show(false)

    //save all matches
    matchesDF.write.mode("overwrite").parquet(outputDir + "-unfiltered")

    //filter matches
    val matchesForSaving = matchesDF.filter($"similarity" >= 0.8 || $"match" === "true")
    matchesForSaving.show(false)
    matchesForSaving.count
    matchesForSaving.select($"subject", $"region_id").write.mode("overwrite").parquet(outputDir + "")


  }

}
