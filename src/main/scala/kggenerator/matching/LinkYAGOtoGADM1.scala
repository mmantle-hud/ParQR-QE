package kggenerator.matching

import info.debatty.java.stringsimilarity.JaroWinkler
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.{lower, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/*
Matches GADM layer 1 regions to YAGO points
 */

object LinkYAGOtoGADM1 {


  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Match YAGO Points to GADM Layer 1")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
      .getOrCreate()


    SedonaSQLRegistrator.registerAll(spark)
    import spark.implicits._

    var regionInfo = spark.read.json(args(0))
    regionInfo.printSchema()
    regionInfo = regionInfo.select(
      $"properties.ENGTYPE_1".as("r_type"),
      $"properties.TYPE_1".as("a_type"),
      $"properties.GID_1".as("gid"),
      $"properties.NAME_1".as("name"),
      $"properties.VARNAME_1".as("varname"),
    ).distinct()

    val pointInMBB = spark.read
      .parquet(args(1))
      .select($"region_id", $"subject")

    println("Point in MBB:" + pointInMBB.count)


    val labels = spark.read.parquet(args(2)).filter($"object".substr(-3, 3) === "@en").distinct
    labels.createOrReplaceTempView("labels")

    val types = spark.read.parquet(args(3))
    types.createOrReplaceTempView("types")

    val dissolution = spark.read.parquet(args(4))
    dissolution.createOrReplaceTempView("dissolution")

    val userType = "<" + args(5) + ">"

    val layer0Matches = spark.read.parquet(args(6))

    val outputDir = args(7)

    //Join points in MBBs to type to get the type info for points
    var pointsInMBBsWithTypeInfo = pointInMBB.as("p")
      .join(types.as("t"), $"p.subject" === $"t.subject")
      .where($"t.object" === userType)
      .select(
        $"p.subject".as("subject"),
        $"p.region_id".as("region_id")
      )

    //left outer join with dissolution data to get current regions only
    pointsInMBBsWithTypeInfo = pointsInMBBsWithTypeInfo.as("c")
      .join(dissolution.as("d"), $"c.subject" === $"d.subject", "left_outer")
      .filter($"d.subject".isNull)
      .select(
        $"c.subject".as("subject"),
        $"c.region_id".as("region_id")
      )

    //left outer join with Layer 0 results so we don't get duplicates
    pointsInMBBsWithTypeInfo = pointsInMBBsWithTypeInfo.as("m1")
      .join(layer0Matches.as("m0"), $"m1.subject" === $"m0.subject", "left")
      .filter($"m0.subject".isNull)
      .select(
        $"m1.subject".as("subject"),
        $"m1.region_id".as("region_id")
      )

    val getShortLabel = udf((label: String) => {
      val shortLabel = label.substring(1, label.indexOf("@en") - 1)
      val normalLabel = if (shortLabel.lastIndexOf(",") > -1)
        shortLabel.substring(0, shortLabel.lastIndexOf(","))
      else {
        shortLabel
      }
      normalLabel.toLowerCase()
    })

    //now join the points to their labels
    val pointsInMBBsWithTypeInfoAndLabels = pointsInMBBsWithTypeInfo.as("r")
      .join(labels.as("l"), $"r.subject" === $"l.subject")
      .select(
        $"r.subject".as("subject"),
        $"r.region_id".as("region_id"),
        //$"r.name".as("name"),
        $"l.object".as("label"),
      )
      .withColumn("label", getShortLabel($"label"))

    //get alternative names for the regions
    def joinNames = udf((name: String, varname: String) => {
      (name + "|" + varname).toLowerCase()
    })
    regionInfo = regionInfo.withColumn("name", joinNames($"name", $"varname")).drop($"varname")

    //join the points to the regions using the region_id from the MBB and the actual region
    var joinedPointsAndRegions = pointsInMBBsWithTypeInfoAndLabels.as("r").join(regionInfo.as("t"), $"r.region_id" === $"t.gid")
    joinedPointsAndRegions = joinedPointsAndRegions.select(
      $"r.subject".as("subject"),
      $"r.region_id".as("region_id"),
      $"t.name".as("name"),
      $"t.r_type".as("gadm_type"),
      $"r.label".as("label"))
      .distinct()
    joinedPointsAndRegions.show()

    //get an array of all region types from GADM - Oblast, Province, Department etc.
    val gadmTypes = joinedPointsAndRegions.select(lower($"gadm_type")).distinct().collect().map(x => x(0))


    //Now strip these GADM types from the labels (improves matching later)
    def stripTypeFromLabel = udf((label: String) => {
      var labelForKeeping = label
      gadmTypes.foreach(gadmType => {
        labelForKeeping = labelForKeeping.replace(gadmType.toString(), "")
      })
      labelForKeeping
    })
    joinedPointsAndRegions = joinedPointsAndRegions.withColumn("label", stripTypeFromLabel($"label"))

    //Now compute the similarity of the various GADM names to the YAGO label and find the value of the best match
    def checkAltNames = udf((names: String, label: String) => {
      val namesArr = names.split("\\|")
      val jw = new JaroWinkler()
      val similarityScores = namesArr.map(n => {
        jw.similarity(n.stripMargin, label.stripMargin)
      })
      val bestScore = similarityScores.reduce((a, b) => {
        if (a > b) a else b
      })
      bestScore
    })
    joinedPointsAndRegions = joinedPointsAndRegions.withColumn("best_score", checkAltNames($"name", $"label"))

    //now try and match using type information from YAGO and GADM
    //first get the labels for YAGO types
    var yagoTypesAndLabels = types.as("t").join(labels.as("l"), $"t.object" === $"l.subject").select(
      $"t.subject".as("subject"),
      $"l.object".as("object")
    )
    yagoTypesAndLabels = yagoTypesAndLabels.withColumn("label", getShortLabel($"object"))

    //join the existing matches to these type labels
    var regionsAndYagoTypes = joinedPointsAndRegions.as("r").join(yagoTypesAndLabels.as("y"), $"r.subject" === $"y.subject").select(
      $"r.subject".as("subject"),
      $"r.region_id".as("region_id"),
      $"r.name".as("name"),
      $"r.gadm_type".as("gadm_type"),
      $"r.label".as("label"),
      $"r.best_score".as("best_score"),
      $"y.label".as("yago_type")
    )



    //Test to see if the GADM type matches the YAGO type
    def hasSameType = udf((gadm_type: String, yago_type: String) => {
      yago_type.toLowerCase() contains gadm_type.toLowerCase()
    })
    regionsAndYagoTypes = regionsAndYagoTypes.withColumn("match", hasSameType($"gadm_type", $"yago_type"))
    regionsAndYagoTypes = regionsAndYagoTypes.drop($"yago_type")
    regionsAndYagoTypes.show()
    regionsAndYagoTypes.cache()

    //Convert to an RDD
    //Reduce the matches on the basis of the correct type and then text similarity
    val regionsRDD = regionsAndYagoTypes.rdd.map(row => {
      (row(1).toString, (row(0).toString, row(1).toString, row(2).toString, row(3).toString, row(4).toString, row(5).toString().toDouble, row(6).toString))
    })

    val reducedByRegionRDD = regionsRDD.reduceByKey((a, b) => {
      val chosen = if (a._7 == "true" && b._7 == "false") {
        a
      } else if (a._7 == "false" && b._7 == "true") {
        b
      } else if (a._6 > b._6) {
        a
      } else {
        b
      }
      chosen
    })

    val reducedBySubjectRDD = reducedByRegionRDD.map(x => (x._2._1, x._2)).reduceByKey((a, b) => {
      val chosen = if (a._7 == "true" && b._7 == "false") {
        a
      } else if (a._7 == "false" && b._7 == "true") {
        b
      } else if (a._6 > b._6) {
        a
      } else {
        b
      }
      chosen
    })

    val reducedRDD = reducedBySubjectRDD.map(x => x._2).map(x => Row(x._1, x._2, x._3, x._4, x._5, x._6.toString, x._7))

    //convert back to a df
    val schemaString = "subject region_id name gadm_type label best_score type_match"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val finalRegions = spark.createDataFrame(reducedRDD, schema)
    finalRegions.show()

    //Save complete final results
    finalRegions.write.mode("overwrite").parquet(outputDir + "-unfiltered")

    //Now filter, need a minimum similarity of 80% and be of the correct type
    val matchesForSaving = finalRegions.filter($"best_score" >= 0.8 && $"type_match" === "true")
    matchesForSaving.show(false)
    matchesForSaving.select($"subject", $"region_id").write.mode("overwrite").parquet(outputDir)

  }

}
