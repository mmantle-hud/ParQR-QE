package kggenerator.delete

import org.apache.sedona.core.serde.SedonaKryoRegistrator
import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession

object ReadPointData {
  def main(args: Array[String]) {

    var spark = SparkSession
      .builder()
      .master("local[4]") // Delete this if run in cluster mode
      .appName("Test we can read saved Sedona points") // Change this to a proper name
      // Enable Sedona custom Kryo serializer
      .config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
      .config("spark.kryo.registrator", classOf[SedonaKryoRegistrator].getName)

      .getOrCreate() // org.apache.sedona.core.serde.SedonaKryoRegistrator

    SedonaSQLRegistrator.registerAll(spark)


    //import spark.implicits._
    //load textfile

    val inputDir = "./output/points-obj/part-00000"
    // val rdd = spark.sparkContext.textFile(inputDir)
    // rdd.take(10).foreach(println)
    //val spatialRDD = new PointRDD(spark.sparkContext.objectFile[Point](inputDir))
    //val spatialDf = Adapter.toDf(spatialRDD, fieldNames = Seq("subject"), spark)
    // spatialDf.show()
    //spatialRDD.rawSpatialRDD.take(10).forEach(println)
    //rdd.take(25).foreach(println)
  }
}
