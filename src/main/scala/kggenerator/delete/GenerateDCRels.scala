package kggenerator.delete

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object GenerateDCRels {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      //.master("local[4]")
      .appName("Generate DC Relations")
      .getOrCreate()
    import spark.implicits._
    //    val regionDir = "gs://mmantle/query-engine/temp/rcc8/ec/full/level1"
    val regionECDir = args(0)
    val outputDir = args(1)

    //not sure this is the correct column identifiers. It might be gid_r.
    val regions = spark.read.parquet(regionECDir).select($"gid1", $"gid2")

    val regionsRDD = regions.rdd.map(x => {
      (x(0).toString, x(1).toString)
    })

    val regionsRDDSorted = regionsRDD.map(x => {
      var r1 = x._1
      var r2 = x._2
      if (x._1 < x._2) {
        r1 = x._2
        r2 = x._1
      }
      (r1, "EC", r2)
    })
    println("There are " + regionsRDDSorted.count + " EC relations before the removal of duplicates")
    val ecRels = regionsRDDSorted.distinct
    ecRels.cache()
    println("There are " + ecRels.count + " EC relations after the removal of duplicates")

    val r1 = regionsRDD.map(x => x._1)
    val r2 = regionsRDD.map(x => x._2)

    val allRegions = r1.union(r2).distinct

    val join = allRegions.cartesian(allRegions).filter(x => x._1 != x._2)

    val joinSorted = join.map(x => {
      var r1 = x._1
      var r2 = x._2
      if (x._1 < x._2) {
        r1 = x._2
        r2 = x._1
      }
      (r1, "DC", r2)
    })

    val dcRels = joinSorted.distinct
    dcRels.cache()
    dcRels.count

    val allRels = ecRels.union(dcRels).map(x => (x._1 + "#" + x._3, x))

    val finalRels = allRels.reduceByKey((a, b) => {
      val rel = if (a._2 == "EC") {
        "EC"
      } else if (b._2 == "EC") {
        "EC"
      } else {
        "DC"
      }
      (a._1, rel, a._3)
    }).map(x => x._2)

    //finalRels.take(20).foreach(println)

    // The schema is encoded in a string
    val schemaString = "gid1 rel gid2"
    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // Convert records of the RDD to Rows
    val rowRDD = finalRels.map(attributes => Row(attributes._1, attributes._2, attributes._3))
    val regionsDF = spark.createDataFrame(rowRDD, schema)
    regionsDF.show()
    println(regionsDF.count)
    regionsDF.write.parquet(outputDir)

  }
}
