package parqrbroadcast

import org.apache.spark.sql.SparkSession

object DictionaryEncoding {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Run Queries")
      .getOrCreate()



    val inputDir = args(0)
    val dictionaryOutputDir = args(1)
    val outputDir = args(2)
    val partitions = args(3).toInt

    val sc = spark.sparkContext

    //process input
    val triples = sc.textFile(inputDir)
      .map(line=>{
    val parts = line.split(" ")
    (parts(0), parts(1).split(",").map(_.toInt).toSet , parts(2), 1)
  })


    println("Number of spatial triples:"+triples.count)

    triples.take(30).foreach(println)
    triples.cache()

    //generate dictionary
    val dictItems=triples.flatMap(triple=>List(triple._1,triple._3))
      .map(item => (item,1))
      .reduceByKey(_+_).map(x=>x._1)
    val dictionary = dictItems.zipWithUniqueId
    dictionary.cache()
    println("There are "+dictionary.count+" entities")
    dictionary.take(30).foreach(x=>print("\n"+x))

    val dictionaryDF = spark.createDataFrame(dictionary).toDF("id","encodedId")

    dictionaryDF.repartition(partitions).write.parquet(dictionaryOutputDir)

    val triplesKeyedByLeft = triples.map(x=>(x._1,x))

    val triplesKeyedByRight = triplesKeyedByLeft
      .join(dictionary)
      .map(x=>(x._2._1._3,(x._2._2,x._2._1._2,x._2._1._3)))

    val encodedTriples = triplesKeyedByRight
      .join(dictionary)
      .map(x=>x._2._1._1+" "+x._2._1._2.mkString(",")+", "+x._2._2)

    encodedTriples.take(20).foreach(println)

    encodedTriples.repartition(partitions).saveAsTextFile(outputDir)

  }
}
