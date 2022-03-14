package parqrbroadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable.HashMap

/*
        1=DC
        2=EC
        3=PO
        4=TPP
        5=NTPP
        6=TPPi
        7=NTPPi
        8=EQ
         */


object ParQR {

  def query(
             spark:SparkSession,
             subjects:Array[String],
             rel:Set[Int],
             calculus:Calculus,
             allTriples:RDD[(Int,Int,Int,Int)],
             //allTriplesDF:DataFrame,
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


    val results = reason(
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



      val resultsDF = spark.createDataFrame(filteredResults).toDF("parent","child")

    resultsDF
      .join(dictionary,$"parent"===$"encodedId")
      .select($"id".as("parent"),$"child")
      .join(dictionary,$"child"===$"encodedId")
      .select($"parent",$"id".as("child"))


  }

  def reason(
              spark:SparkSession,
              subjects:Array[Int],
              calculus:Calculus,
              allTriples:RDD[(Int,Int,Int,Int)],
              //allTriplesDF:DataFrame,
              keyedTriplesB:Broadcast[scala.collection.Map[Int,List[(Int,Int,Int,Int)]]],
              compTblB:Broadcast[HashMap[Int, HashMap[Int,Int]]],
              intersectTblB:Broadcast[HashMap[Int,HashMap[Int,Int]]],
              relSizeTblB:Broadcast[HashMap[Int,Int]],
              partitions:Int
            ):RDD[(Int,Int,Int,Int)] = {

    import org.apache.spark.sql.functions._
    import spark.implicits._


    val universalRel = calculus.universalRel
    val inconsistentRels = spark.sparkContext.longAccumulator("Inconsistent rels")

    val queryTriples = allTriples
      .filter((triple) => subjects contains triple._1)



    val queryTriplesDF = queryTriples
      .toDF("lhs", "rel", "rhs", "pathLength")
      .repartition(partitions, $"lhs")
      .cache()

    println("lhs count:"+queryTriplesDF.count)




    val results = queryTriplesDF.rdd.map(x => {
      (
        x(0).asInstanceOf[Int],
        x(1).asInstanceOf[Int],
        x(2).asInstanceOf[Int],
        x(3).asInstanceOf[Int],
        )
    })
      .mapPartitions((iterator) => {
        var numIts = 1
        var continue = true
        val keyedTriples = keyedTriplesB.value
        val compTbl = compTblB.value
        val intersectTbl = intersectTblB.value
        val relSizeTbl = relSizeTblB.value
        var queryTriples = iterator.toList
        var currentCount = queryTriples.size
        while (continue) {
          val lhsTriples = queryTriples.filter(_._4 == numIts)
          val results = lhsTriples.map(lhsTriple => {
            val joinkey = lhsTriple._3
            val rhsTriples = keyedTriples(joinkey)
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
              .filter(triple => triple._2 != universalRel)
            newRels
          }).flatMap(x => x)

          val itr = numIts+1
          queryTriples = results.union(queryTriples)
            .map(triple => (triple._1 + "#" + triple._3, triple))
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
                  if(sizeiSect==0){
                     inconsistentRels.add(1)
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
            })
            .map(keyValuePair => keyValuePair._2).toList

          val newCount = queryTriples.size

          //println("Old count:" + currentCount)
          //println("New count:" + newCount)
          if (newCount.equals(currentCount)) {
            continue = false
          }
          else {
            numIts += 1
            currentCount = newCount
          }
        } //end of while loop
        Iterator(queryTriples)
      }).flatMap(x => x)
      .cache()//need to remove this



    println("Final results count:"+results.count)
    //results.take(10).foreach(println)
    results

  }
}
