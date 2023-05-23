package kggenerator.delete

import org.apache.spark.sql.SparkSession

object TrimGADM {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .master("local[4]") // Delete this if run in cluster mode
      .appName("Trim GADM") // Change this to a proper name
      .getOrCreate() // org.apache.sedona.core.serde.SedonaKryoRegistrator

    val fraRDD = spark.sparkContext.textFile("C:\\work\\sedona\\data\\geoms\\gadm\\usa.json")
    fraRDD.cache()
    val rddRowCount = fraRDD.count
    //fraRDD.repartition(1).take(10).foreach(println)
    val rddWithIndices = fraRDD.zipWithIndex()
    val filteredRddWithIndices = rddWithIndices.filter(eachRow =>
      if (eachRow._2 == 0) false
      else if (eachRow._2 == rddRowCount - 1) false
      else true
    )
    val finalRDD = filteredRddWithIndices.map(eachRow => eachRow._1)
    println(finalRDD.count)
    val states = Array("California")
    val chosen = finalRDD.filter(line =>
      ((line contains "California") ||
        (line contains "Florida") ||
        (line contains "New York")))

    println(chosen.count)
    chosen.repartition(1).saveAsTextFile("C:\\work\\sedona\\data\\geoms\\gadm\\usa-trimmed.json")
  }
}
