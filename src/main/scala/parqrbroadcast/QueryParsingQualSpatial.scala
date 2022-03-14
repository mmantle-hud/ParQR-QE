package parqrbroadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable.{HashMap, Map}

object QueryParsingQualSpatial {
  def runQuery(
               spark:SparkSession,
               queryTriple:(String,String,String),
               nonSpatialResults:Map[String, DataFrame],
               calculus:Calculus,
               allTriples:RDD[(Int,Int,Int,Int)],
               keyedTriplesB:Broadcast[scala.collection.Map[Int,List[(Int,Int,Int,Int)]]],
               dictionary:DataFrame,
               compTblB:Broadcast[HashMap[Int, HashMap[Int,Int]]],
               intersectTblB:Broadcast[HashMap[Int,HashMap[Int,Int]]],
               relSizeTblB:Broadcast[HashMap[Int,Int]],
               partitions:Int
    ):Map[String, DataFrame]={
  import spark.implicits._
    //queryTriple (var, rel, spatialObject)  e.g. (?city, 4,5, <http://yago-knowledge.org/resource/Mexico>)
    val rel=queryTriple._2



      var setRel = if(rel=="sfTouches" || rel=="sf-touches") {
        Set(2)
      }else{
        Set(6,7)
      }

      println("\nRunning query:"+queryTriple)
      var queryVar = queryTriple._1.substring(1)
      var spatialObjectVar = queryTriple._3.substring(1)
      var spatialJoin = false

      val spatialObjects = if(queryTriple._3.indexOf("?") > -1){
        //it's a join
        spatialJoin = true

        //we reason using the parents
        nonSpatialResults(spatialObjectVar)
          .select(col(spatialObjectVar).as("spatialObject"))
          .distinct.collect.map(x=>x(0).toString())
      }else{

        spatialObjectVar = queryTriple._3
        Array(queryTriple._3)
      }

      println(queryTriple)
      println("Spatial object var:"+spatialObjectVar)
      println("Spatial objects count:"+spatialObjects.size)



      var results = ParQR.query(
        spark,
        spatialObjects,
        setRel,
        calculus,
        allTriples,
        keyedTriplesB,
        dictionary,
        compTblB,
        intersectTblB,
        relSizeTblB,
        partitions
      )

    results = if(rel == "sfWithin" || rel == "sf-within"){
        val spatialObjectsRDD = spark.sparkContext.parallelize(spatialObjects).map(x=>(x,x))

        results.union(spatialObjectsRDD.toDF("parent","child"))
      }else{
        results
      }



      if(spatialJoin){
          val join = results
            .join(nonSpatialResults(spatialObjectVar),$"parent"===col(spatialObjectVar))
            .drop($"parent")
            .join(nonSpatialResults(queryVar),$"child"===col(queryVar))
            .drop("child")

          join.columns.toSeq.foreach(col=>{
            nonSpatialResults(col) = join
          })

        }else{
          val join = results
            .join(nonSpatialResults(queryVar),$"child"===col(queryVar))
            .drop("child")
            .drop("parent")

          join.columns.toSeq.foreach(col=>{
            nonSpatialResults(col) = join
          })

  }

  nonSpatialResults


}

}
