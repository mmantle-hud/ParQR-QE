package kggenerator.delete

import info.debatty.java.stringsimilarity.JaroWinkler
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.{desc, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object LinkYAGOtoGADM {

  /*
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      //.master("local[4]")
      .appName("Match YAGO Points to GADM")
      .config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
      .getOrCreate() // org.apache.sedona.core.serde.SedonaKryoRegistrator


    SedonaSQLRegistrator.registerAll(spark)
    import spark.implicits._

    //region data
    //|region_id|geometry|name|
    //point data
    //|subject|geometry|
    //rdf:type info
    //|subject|object|
    //rdfs:label
    //|subject|object|


    //var regions = spark.read.parquet("./data/output/regions/parquet/NE/layer0")
    //var regions = spark.read.parquet("./data/output/regions/parquet/GADM/level1-fra-bel")
    var regions = spark.read.parquet(args(0))
    regions.createOrReplaceTempView("regions")

//    var centroids = spark.read.parquet("./data/output/centroids/level0")
    //var centroids = spark.read.parquet("./data/output/centroids/level1-fra-bel-che")
    var centroids = spark.read.parquet(args(1))
    centroids.createOrReplaceTempView("centroids")

    //var points = getPoints(spark)
    var points = spark.read.parquet(args(2))
    points.createOrReplaceTempView("pointtable")

    //val labels = getLabels(spark)
    var labels = spark.read.parquet(args(3)).filter($"object".substr(-3,3)==="@en").distinct
    labels.createOrReplaceTempView("labels")

    //val types = getTypes(spark)
   val types = spark.read.parquet(args(4))
    types.createOrReplaceTempView("types")

    //val dissolution = getDissolution(spark)
    val dissolution = spark.read.parquet(args(5))
    dissolution.createOrReplaceTempView("dissolution")

//    val userType = "<http://schema.org/AdministrativeArea>" //"http://schema.org/Country"
    val userType = "<"+args(6)+">" //"http://schema.org/Country"

//    val outputDir = "./data/output/matches/layer0"
    val outputDir = args(7) //"./data/output/matches/layer0"

    points = spark.sql(
      """
        | SELECT ST_GeomFromWKT(pointtable.geometry) AS geometry, subject
        | FROM pointtable
        """.stripMargin)
    points.createOrReplaceTempView("pointtable")
    points.cache()
    points.show(false)
    println("There are "+points.count+" total points")


    // join points to types to get points of correct type only
    val pointsByType = points.as("p")
      .join(types.as("t"),$"p.subject"===$"t.subject")
      .where($"t.object" === userType)
      .select(
        $"p.subject".as("subject"),
        $"p.geometry".as("geometry")
      )
    println("There are "+pointsByType.count+" of type "+userType)
    pointsByType.show(false)



    //left outer join with dissolution data to get current regions only
    var currentPoints = pointsByType.as("c")
      .join(dissolution.as("d"), $"c.subject" === $"d.subject", "left_outer")
      .filter($"d.subject".isNull)
      .select(
        $"c.subject".as("subject"),
        $"c.geometry".as("geometry")
      )

    println("Current YAGO points of type"+userType)
    currentPoints.createOrReplaceTempView("pointtable")
    currentPoints.show(false)
    println("There are "+currentPoints.count+" after removal of dissolution")
    //now join regions to points to find points located within regions
    regions.createOrReplaceTempView("regiontable")
    regions = spark.sql(
      """
        | SELECT ST_GeomFromWKT(regiontable.geometry) AS geometry, region_id, name
        | FROM regiontable
        """.stripMargin)
    regions.createOrReplaceTempView("regiontable")
    regions.cache()
    regions.show()


    var resultsDF = spark.sql(
      "SELECT regiontable.region_id, regiontable.name, pointtable.subject FROM regiontable, pointtable " +
        "WHERE ST_Contains(regiontable.geometry,pointtable.geometry)"
    )
    resultsDF.cache()
    resultsDF.show(false)

    println("There are "+resultsDF.count+" computed relations i.e. points within regions")

    println("Looking for Grand Est...")
    resultsDF.filter($"subject" contains "Grand_Est>").show(50,false)

    //now left outer join to find YAGO points that haven't been placed.
    var unplacedPoints = currentPoints.as("c")
      .join(resultsDF.as("r"), $"c.subject" === $"r.subject", "left_outer")
      .filter($"r.subject".isNull)
      .select(
        $"c.subject".as("subject"),
        $"c.geometry".as("geometry")
      )

    println("Unplaced points:")
    unplacedPoints.show(false)
    println("There are "+unplacedPoints.count+" unplaced points")

    centroids.createOrReplaceTempView("centroids")
    centroids = spark.sql(
      """
        | SELECT ST_GeomFromWKT(centroids.centroid) AS centroid, region_id, name
        | FROM centroids
        """.stripMargin)
    centroids.createOrReplaceTempView("centroids")
    centroids.cache()
    centroids.show()

    val unplacedPointsArr = unplacedPoints.rdd.map(row=>{
      (row(0).toString(),row(1).toString)
    }).collect()

    val closest = unplacedPointsArr.map(unplacedCountry=>{
      val geom = unplacedCountry._2
      val knn = spark.sql(
          "SELECT centroids.region_id, centroids.name, " +
          "ST_Distance(ST_GeomFromWKT('"+geom+"'), centroids.centroid) AS distance "+
          "FROM centroids ORDER BY distance ASC LIMIT 1")

      val result = knn.rdd.map(r=>(r(0), r(1), r(2))).collect()(0)
      (result._1.toString, result._2.toString, unplacedCountry._1.toString)
    })


    val closestRDD = spark.sparkContext.parallelize(closest)

    closestRDD.foreach(x=>println(x._1+" "+x._2+" "+x._3))

    val rowRDD = closestRDD.map(x=>(Row(x._1,x._2,x._3)))
    val schemaString = "region_id name subject"
    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val closestDF = spark.createDataFrame(rowRDD,schema)

    closestDF.show(false)

    resultsDF = resultsDF.union(closestDF)

    resultsDF.show(false)

    val getShortLabel = udf((label:String)=> {
      val shortLabel = label.substring(1, label.indexOf("@")-1)
      val normalLabel = if(shortLabel.lastIndexOf(",") > -1 )
        shortLabel.substring(0,shortLabel.lastIndexOf(","))
      else{
        shortLabel
      }
      normalLabel
    })

    def findNameInLabel = udf((subject:String, name:String)=>{
      val parts = name.split(" ")
      var found = true
      parts.foreach(part=>{
        if (!(subject contains part)) found =false
      })
      found
    })

    def computeSimilarity = udf((subject:String, name:String)=>{
      val jw = new JaroWinkler()
      jw.similarity(name, subject)
      //val lev = new Levenshtein()
      //lev.distance(name, subject)
    })
    //now join spatial results to the labels
    val resultsAndLabels = resultsDF.as("r")
      .join(labels.as("l"), $"r.subject" === $"l.subject")
      .select(
        $"r.subject".as("subject"),
        $"r.region_id".as("region_id"),
        $"r.name".as("name"),
        $"l.object".as("label"),
      )
    .withColumn("label",getShortLabel($"label"))
    .withColumn("match",findNameInLabel($"label",$"name"))
    .withColumn("similarity",computeSimilarity($"label",$"name"))
    .repartition(1)
    .orderBy(desc("similarity"))

    println("Matches over 0.99 is "+resultsAndLabels.filter($"similarity" > 0.99).count)
    resultsAndLabels
      .show(false)

    resultsAndLabels.write.parquet(outputDir)
/*
   val forPrinting = resultsAndLabels.rdd.map(row=>row(0)+" "+row(1)+" "+row(2)+" "+row(3)+" "+row(4))

  forPrinting.coalesce(1).saveAsTextFile(outputDir)
*/
  }
*/


  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      //      .master("local[4]")
      .appName("Match YAGO Points to GADM")
      .config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
      .getOrCreate() // org.apache.sedona.core.serde.SedonaKryoRegistrator


    SedonaSQLRegistrator.registerAll(spark)
    import spark.implicits._
    /*
    //region data
    |region_id|geometry|name|
    //point data
    |subject|geometry|
    //rdf:type info
    |subject|object|
    //rdfs:label
    |subject|object|
     */

    //    var pointInPolygon = spark.read.parquet("./data/output/points-in-regions")
    var pointInPolygon = spark.read.parquet(args(0)).distinct()
    pointInPolygon.createOrReplaceTempView("regions")

    //    var centroids = spark.read.parquet("./data/output/centroids/level1-fra-bel-che")
    var centroids = spark.read.parquet(args(1))
    centroids.createOrReplaceTempView("centroids")

    //    var points = getPoints(spark)
    var points = spark.read.parquet(args(2))
    points.createOrReplaceTempView("pointtable")

    //    val labels = getLabels(spark)
    var labels = spark.read.parquet(args(3)).filter($"object".substr(-3, 3) === "@en").distinct
    labels.createOrReplaceTempView("labels")

    //    val types = getTypes(spark)
    val types = spark.read.parquet(args(4))
    types.createOrReplaceTempView("types")

    //    val dissolution = getDissolution(spark)
    val dissolution = spark.read.parquet(args(5))
    dissolution.createOrReplaceTempView("dissolution")

    //    val userType = "<http://schema.org/AdministrativeArea>"
    val userType = "<" + args(6) + ">" //"http://schema.org/Country"

    //    val outputDir = "./data/output/matches/layer0"
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

    //now join to the point in polygon dataset

    //|region_id|name|subject|
    var resultsDF = currentPoints.as("c")
      .join(pointInPolygon.as("p"), $"c.subject" === $"p.subject")
      .select(
        $"p.region_id".as("region_id"),
        $"p.name".as("name"),
        $"c.subject".as("subject")
      )

    //now left outer join to find YAGO points that haven't been placed.
    var unplacedPoints = currentPoints.as("c")
      .join(resultsDF.as("r"), $"c.subject" === $"r.subject", "left_outer")
      .filter($"r.subject".isNull)
      .select(
        $"c.subject".as("subject"),
        $"c.geometry".as("geometry")
      )

    println("Unplaced points:")
    unplacedPoints.show(false)
    println("There are " + unplacedPoints.count + " unplaced points")

    centroids.createOrReplaceTempView("centroids")
    centroids = spark.sql(
      """
        | SELECT ST_GeomFromWKT(centroids.centroid) AS centroid, region_id, name
        | FROM centroids
        """.stripMargin)
    centroids.createOrReplaceTempView("centroids")
    centroids.cache()
    centroids.show()

    val unplacedPointsArr = unplacedPoints.rdd.map(row => {
      (row(0).toString(), row(1).toString)
    }).collect()

    val closest = unplacedPointsArr.map(unplacedCountry => {
      val geom = unplacedCountry._2
      val knn = spark.sql(
        "SELECT centroids.region_id, centroids.name, " +
          "ST_Distance(ST_GeomFromWKT('" + geom + "'), centroids.centroid) AS distance " +
          "FROM centroids ORDER BY distance ASC LIMIT 1")

      val result = knn.rdd.map(r => (r(0), r(1), r(2))).collect()(0)
      (result._1.toString, result._2.toString, unplacedCountry._1.toString)
    })


    val closestRDD = spark.sparkContext.parallelize(closest)

    closestRDD.foreach(x => println(x._1 + " " + x._2 + " " + x._3))

    val rowRDD = closestRDD.map(x => (Row(x._1, x._2, x._3)))
    val schemaString = "region_id name subject"
    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val closestDF = spark.createDataFrame(rowRDD, schema)

    closestDF.show(false)

    resultsDF = resultsDF.union(closestDF)

    resultsDF.show(false)

    val getShortLabel = udf((label: String) => {
      val shortLabel = label.substring(1, label.indexOf("@") - 1)
      val normalLabel = if (shortLabel.lastIndexOf(",") > -1)
        shortLabel.substring(0, shortLabel.lastIndexOf(","))
      else {
        shortLabel
      }
      normalLabel
    })

    def jaroWinkler = udf((subject: String, name: String) => {
      val jw = new JaroWinkler()
      jw.similarity(name, subject)
    })

    //now join spatial results to the labels
    val resultsAndLabels = resultsDF.as("r")
      .join(labels.as("l"), $"r.subject" === $"l.subject")
      .select(
        $"r.subject".as("subject"),
        $"r.region_id".as("region_id"),
        $"r.name".as("name"),
        $"l.object".as("label"),
      )
      .withColumn("label", getShortLabel($"label"))
      .withColumn("similarity", jaroWinkler($"label", $"name"))
      .repartition(1)
      .orderBy(desc("similarity"))

    println("Matches over 0.99 is " + resultsAndLabels.filter($"similarity" > 0.99).count)
    resultsAndLabels
      .show(false)

    resultsAndLabels.write.parquet(outputDir)
    /*
    val forPrinting = resultsAndLabels.rdd.map(row=>row(0)+" "+row(1)+" "+row(2)+" "+row(3)+" "+row(4))

    forPrinting.coalesce(1).saveAsTextFile(outputDir)
    */
  }


  def getPoints(spark: SparkSession): DataFrame = {
    val pointRDD = spark.sparkContext.parallelize(Array(
      ("POINT (2.000 47.000)", "<http://yago-knowledge.org/resource/France>"),
      ("POINT (4.6680555 50.64111)", "<http://yago-knowledge.org/resource/Belgium>"),
      ("POINT (5.55 52.316)", "<http://yago-knowledge.org/resource/Netherlands>"),
      ("POINT (108.00 3.00)", "<http://yago-knowledge.org/resource/Malaysia>"),
      ("POINT (-77.036 38.89500)", "<http://yago-knowledge.org/resource/United_States>"),
      ("POINT (49.88333 40.36666)", "<http://yago-knowledge.org/resource/Azerbaijan_Soviet_Socialist_Republic>"),
      ("POINT (47.7000 40.3000)", "<http://yago-knowledge.org/resource/Azerbaijan>"),
      ("POINT (7.7599 48.5989)", "<http://yago-knowledge.org/resource/Grand_Est>"),
      ("POINT (6.0305 47.23452778)", "<http://yago-knowledge.org/resource/Bourgogne-Franche-Comté>")
    )).map(attributes => Row(attributes._1, attributes._2))
    // The schema is encoded in a string
    val schemaString = "geometry subject"
    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    spark.createDataFrame(pointRDD, schema)
  }

  def getTypes(spark: SparkSession): DataFrame = {
    val typeRDD = spark.sparkContext.parallelize(Array(
      ("<http://yago-knowledge.org/resource/France>", "<http://schema.org/Country>"),
      ("<http://yago-knowledge.org/resource/Belgium>", "<http://schema.org/Country>"),
      ("<http://yago-knowledge.org/resource/Netherlands>", "<http://schema.org/Country>"),
      ("<http://yago-knowledge.org/resource/Malaysia>", "<http://schema.org/Country>"),
      ("<http://yago-knowledge.org/resource/United_States>", "<http://schema.org/Country>"),
      ("<http://yago-knowledge.org/resource/Azerbaijan_Soviet_Socialist_Republic>", "<http://schema.org/Country>"),
      ("<http://yago-knowledge.org/resource/Azerbaijan>", "<http://schema.org/Country>"),
      ("<http://yago-knowledge.org/resource/Grand_Est>", "<http://schema.org/Place>"),
      ("<http://yago-knowledge.org/resource/Grand_Est>", "<http://yago-knowledge.org/resource/Regions_of_France>"),
      ("<http://yago-knowledge.org/resource/Bourgogne-Franche-Comté>", "<http://yago-knowledge.org/resource/Regions_of_France>"),
      ("<http://yago-knowledge.org/resource/Bourgogne-Franche-Comté>", "<http://schema.org/Thing>"),
      ("<http://yago-knowledge.org/resource/Grand_Est>", "<http://schema.org/Thing>"),
      ("<http://yago-knowledge.org/resource/Bourgogne-Franche-Comté>", "<http://schema.org/AdministrativeArea>"),
      ("<http://yago-knowledge.org/resource/Grand_Est>", "<http://schema.org/AdministrativeArea>")
    )).map(attributes => Row(attributes._1, attributes._2))
    // The schema is encoded in a string
    val schemaString = "subject object"
    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    spark.createDataFrame(typeRDD, schema)
  }

  def getLabels(spark: SparkSession): DataFrame = {
    val labelRDD = spark.sparkContext.parallelize(Array(
      ("\"France\"@en", "<http://yago-knowledge.org/resource/France>"),
      ("\"Belgium\"@en", "<http://yago-knowledge.org/resource/Belgium>"),
      ("\"Netherlands\"@en", "<http://yago-knowledge.org/resource/Netherlands>"),
      ("\"Malaysia\"@en", "<http://yago-knowledge.org/resource/Malaysia>"),
      ("\"United States of America\"@en", "<http://yago-knowledge.org/resource/United_States>"),
      ("\"Azerbaijan SSR\"@en", "<http://yago-knowledge.org/resource/Azerbaijan_Soviet_Socialist_Republic>"),
      ("\"Azerbaijan\"@en", "<http://yago-knowledge.org/resource/Azerbaijan>"),
      ("\"Bourgogne-Franche-Comté\"@en", "<http://yago-knowledge.org/resource/Bourgogne-Franche-Comté>"),
      ("\"Grand Est\"@en", "<http://yago-knowledge.org/resource/Grand_Est>")
    )).map(attributes => Row(attributes._1, attributes._2))
    // The schema is encoded in a string
    val schemaString = "object subject"
    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    spark.createDataFrame(labelRDD, schema)
  }

  def getDissolution(spark: SparkSession): DataFrame = {
    val disRDD = spark.sparkContext.parallelize(Array(
      ("\"1867-06-08\"^^xsd:date", "<http://yago-knowledge.org/resource/Azerbaijan_Soviet_Socialist_Republic>")
    )).map(attributes => Row(attributes._1, attributes._2))
    // The schema is encoded in a string
    val schemaString = "object subject"
    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    spark.createDataFrame(disRDD, schema)
  }
}
