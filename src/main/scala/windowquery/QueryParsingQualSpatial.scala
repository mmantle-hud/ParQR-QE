package windowquanfirst

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import parqrbroadcast.{Calculus, ParQR}

import scala.collection.mutable.HashMap

object QueryParsingQualSpatial {
def reason(
            spark:SparkSession,
            subjects:Array[String],
            rel:Set[Int],
            calculus:Calculus,
            allTriples:RDD[(Int,Int,Int,Int)],
            keyedTriplesB:Broadcast[scala.collection.Map[Int,List[(Int,Int,Int,Int)]]],
            dictionary:DataFrame,
            compTblB:Broadcast[HashMap[Int, HashMap[Int,Int]]],
            intersectTblB:Broadcast[HashMap[Int,HashMap[Int,Int]]],
            relSizeTblB:Broadcast[HashMap[Int,Int]],
            partitions:Int
          ):DataFrame = {

  import spark.implicits._

  val subjectsAsInts = dictionary.filter($"id".isin(subjects:_*))
    .select($"encodedId")
    .collect().map(x=>x(0).asInstanceOf[Long].toInt)

  val results = ParQR.reason(
    spark,
    subjectsAsInts,
    calculus,
    allTriples,
    keyedTriplesB,
    compTblB,
    intersectTblB,
    relSizeTblB,
    partitions
  )
  //results
  val intToSetLookUp = calculus.intToSetLookUp

  val inconsistent = results
    .map(x=> {
      (x._1,intToSetLookUp(x._2),x._3)
    }).filter(x=> x._2.size == 0)

  println("Inconsistent count:"+inconsistent.count)

  val filteredResults = results
    .map(x=> {
      (x._1,intToSetLookUp(x._2),x._3)
    })
    .filter(x=> x._2.intersect(rel).size > 0 && x._2.size <= rel.size)
    .map(x=>(x._1,x._3))



  var resultsDF = spark.createDataFrame(filteredResults).toDF("parent","child")

  //add the query objects to the results
  // e.g. Detective Fiction in Sweden
  val spatialObjectsRDD = spark.sparkContext.parallelize(subjects).map(x=>(x,x))
  resultsDF = resultsDF.union(spatialObjectsRDD.toDF("parent","child"))
  resultsDF
}
}
