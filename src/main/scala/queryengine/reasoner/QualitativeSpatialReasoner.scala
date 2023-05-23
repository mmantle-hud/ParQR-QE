package queryengine.reasoner


import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.HashMap

object QualitativeSpatialReasoner{

  //executes backward-chaining for the specified query objects
  def reason(
             spark:SparkSession,
             allTriples:RDD[(Int,Int,Int,Int)],
             keyedTriplesB:Broadcast[scala.collection.Map[Int,List[(Int,Int,Int,Int)]]],
             dictionary:DataFrame,
             compTblB:Broadcast[HashMap[Int, HashMap[Int,Int]]],
             intersectTblB:Broadcast[HashMap[Int,HashMap[Int,Int]]],
             relSizeTblB:Broadcast[HashMap[Int,Int]],
             calculus: Calculus,
             queryObjects:Array[Int],
             //rel:Set[Int],
             partitions:Int
           ):RDD[(Int,Int,Int,Int)]= {

    import spark.implicits._

    val universalRel = calculus.universalRel

    //filter the RCC-8 network to get edges that feature the query objects
    val queryTriples = allTriples
      .filter((triple) => queryObjects contains triple._1)

    //repartition triples that feature the query objects
    val queryTriplesDF = queryTriples
      .toDF("lhs", "rel", "rhs", "pathLength")
      .repartition(partitions, $"lhs")
      .cache()

    //convert back to an RDD
    val partitionedQueryTriples = queryTriplesDF.rdd.map(x => {
      (
        x(0).asInstanceOf[Int],
        x(1).asInstanceOf[Int],
        x(2).asInstanceOf[Int],
        x(3).asInstanceOf[Int],
      )
    })

    //now reason within partitions
    val results = partitionedQueryTriples.mapPartitions((iterator) => {
      var numIts = 1
      var continue = true
      var queryEdges = iterator.toList
      var currentCount = queryEdges.size
      while (continue) {
        val newEdges = inference(queryEdges, keyedTriplesB, compTblB, universalRel, numIts)
        val allEdges = newEdges.union(queryEdges)
        queryEdges = consistency(spark, allEdges, intersectTblB, relSizeTblB, numIts)
        val newCount = queryEdges.size
        if (newCount.equals(currentCount)) {
          continue = false
        }
        else {
          numIts += 1
          currentCount = newCount
        }
      } //end of while loop
      Iterator(queryEdges)
    }).flatMap(x => x)
      .cache()

    results
  }
  def inference(queryTriples:List[(Int,Int,Int,Int)],
                keyedTriplesB:Broadcast[scala.collection.Map[Int,List[(Int,Int,Int,Int)]]],
                compTblB:Broadcast[HashMap[Int, HashMap[Int,Int]]],
                universalRel:Int,
                numIts:Int):List[(Int,Int,Int,Int)]={

    //only want to reason using edges
    val lhsTriples = queryTriples.filter(_._4 == numIts)
    val keyedTriples = keyedTriplesB.value
    val compTbl = compTblB.value
    val results = lhsTriples.map(lhsTriple => {
      val joinkey = lhsTriple._3
      //get edges that can be joined
      val rhsTriples = keyedTriples(joinkey)
      //join edges and look-up the inferred relation
      val newRels = rhsTriples.map(rhsTriple => {
        (lhsTriple, rhsTriple)
      }).filter(pair => {
        val lhsTriple = pair._1
        val rhsTriple = pair._2
        lhsTriple._1 != rhsTriple._3 //no joins to self
      }).map(pair => {
        val lhsTriple = pair._1
        val rhsTriple = pair._2
        val inferredRel = compTbl(lhsTriple._2)(rhsTriple._2)
        (lhsTriple._1, inferredRel, rhsTriple._3, (lhsTriple._4 + rhsTriple._4))
      })
        .filter(triple => triple._2 != universalRel) //filter out universal relations
      newRels
    }).flatMap(x => x)
    results
  }

  def consistency(spark:SparkSession,
                  allEdges:List[(Int,Int,Int,Int)],
                  intersectTblB:Broadcast[HashMap[Int,HashMap[Int,Int]]],
                  relSizeTblB:Broadcast[HashMap[Int,Int]],
                  numIts:Int
                 ):List[(Int,Int,Int,Int)]={
    val intersectTbl = intersectTblB.value
    val relSizeTbl = relSizeTblB.value
    val queryTriples = allEdges.map(triple => (triple._1 + "#" + triple._3, triple))
      .groupBy(_._1)
      .map({
        case(k,v)=>{
          v.reduce((triple1, triple2)=>{
            val a = triple1._2
            val b = triple2._2
            val sizeA = relSizeTbl(a._2)
            val sizeB = relSizeTbl(b._2)
            val iSect = intersectTbl(a._2)(b._2)
            val sizeiSect = relSizeTbl(iSect)
            if(sizeiSect == 0){
              spark.stop()
              println("inconsistent")
            }
            val pathLen = if(a._4 == (numIts+1) && sizeB>sizeiSect){ //the new relation is smaller, so replace
              a._4
            }else if(b._4 == (numIts+1) && sizeA>sizeiSect) { //the new relation is smaller, so replace
              b._4
            }else{ //the new relation is the same size or bigger, so take the min
              Math.min(a._4,b._4)
            }
            (triple1._1,(a._1, iSect, a._3, pathLen))
          })
        }
      }).map(keyValuePair => keyValuePair._2).toList
    queryTriples
  }

}
