package kggenerator.parsers

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/*
Accepts N-Triples files as input
Outputs a parquet table for each predicate in the input graph in the form

 */

object ParseYAGO {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("Parse YAGO")
      .master("local[4]")
      .appName("Parse YAGO")
      .getOrCreate()


    import spark.implicits._

    val inputDir = args(0)
    val outputDir = args(1)


    val files = spark.sparkContext.textFile(inputDir)


    val tuples = files
      .filter(x=>x.length>0)
      .filter(x=>x(0)=='<')
      .map(x=>x.split('\t'))
      .map(x=>(x(0),x(1),x(2)))

    tuples.cache()
    println(tuples.count())
    tuples.take(10).foreach(println)



    //collect the predicates
    val predicates = tuples
      .map(tuple=>tuple._2)
      .distinct()
      .collect()

    println("Pred count:"+predicates.size)

    //create the schema
    val schema = StructType(Seq(
      StructField("subject", StringType, nullable = true),
      StructField("object", StringType, nullable = true)
    ))

    //store VP tables in Parquet
    predicates.foreach(pred=>{
      val matchingTuples = tuples.filter(tuple=>tuple._2==pred)
      val rowRDD = matchingTuples.map(tuple=>Row(tuple._1,tuple._3))
      val df = spark.createDataFrame(rowRDD, schema)
      var predStr = pred.toString()
      val pedStrNoHttp = predStr.substring(8)
      val dir = pedStrNoHttp.substring(0,pedStrNoHttp.indexOf("/"))
      predStr = if(predStr.indexOf("#") > -1){
        val end = predStr.split("#")(1) // e.g. #type>
        end.substring(0,end.length-1)
      }else {
        val lastIndex = predStr.lastIndexOf("/")
        predStr.substring(lastIndex+1,predStr.length-1)
      }

      df.write.format("parquet").mode("overwrite").save(outputDir+dir+"/"+predStr)

    })

  }
}
