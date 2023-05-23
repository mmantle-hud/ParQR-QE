package queryengine.reasoner

import org.apache.spark.rdd.RDD

import scala.collection.mutable.{BitSet, HashMap}

object PreProcessFncs {
  //convert input lines into tuples of the form (Int,BitSet,Int,Int)
  def convertInput(lines:RDD[String]): RDD[(Int,BitSet,Int,Int)] ={
    val inputRels = lines.map(line=>{
      val parts = line.split(" ")
      (parts(0).toInt, parts(1), parts(2).toInt, 1)
    }).map(x=>{
      var rel = new BitSet()
      x._2.split(",").foreach(x=>{
        rel += x.toInt
      })
      (x._1, rel, x._3, x._4)
    })
    inputRels
  }

  //get the inverse of the input QCN
  def getTranspose(inputRels:RDD[(Int,BitSet,Int,Int)],calculus: Calculus): RDD[(Int,BitSet,Int,Int)] ={
    //get the inverse
    val inverseLookUp = calculus.inverseRels
    val inverseRels = inputRels.map(x=>{
      val invs=BitSet()
      x._2.foreach(x=>invs+=inverseLookUp(x))
      (x._3, invs, x._1, x._4)
    })
    inverseRels
  }

  //based on an input network get all possible relations that could arise via composition
  def generatePossibleRelations(iRels:RDD[(Int,BitSet,Int,Int)],
                                calculus:Calculus): Set[BitSet] ={

    var inputRels = iRels.map(x=>x._2).distinct().collect().toSet
    var relCount = inputRels.size
    var continue=true
    while(continue){
      var newRels = Set[BitSet]()
      inputRels.foreach(rel1=>{
        inputRels.foreach(rel2=>{
          val newRel=BitSet()
          rel1
            .map(r1=>rel2
              .map(r2=> newRel ++= calculus.basicCompTbl(r1)(r2)
              )
            )
          if(newRel.size<calculus.basicRelSize) newRels += newRel
        })
      })
      inputRels=inputRels.union(newRels)
      var intNewRels = Set[BitSet]()
      inputRels.foreach(rel1=>{
        inputRels.foreach(rel2=>{
          val newRel = rel1&rel2
          intNewRels+=newRel
        })
      })
      inputRels=inputRels.union(intNewRels)
      if(inputRels.size==relCount){
        continue=false
      }else{
        relCount=inputRels.size
      }
    }
    inputRels
  }

  //generate a complete composition table for all possible relations
  def preComputeCompTbl(inputRels:Set[BitSet],calculus:Calculus): HashMap[Int, HashMap[Int,Int]] ={
    var compTbl = HashMap[Int, HashMap[Int,Int]]()
    inputRels.foreach(rel1=>{
      var partialCompTbl = HashMap[Int,Int]()
      inputRels.foreach(rel2=>{
        val newRel=BitSet()
        rel1
          .map(r1=>rel2
            .map(r2=> newRel ++= calculus.basicCompTbl(r1)(r2)
            )
          )
        partialCompTbl += (calculus.setToIntLookUp(rel2) ->calculus.setToIntLookUp(newRel))
      })
      compTbl += (calculus.setToIntLookUp(rel1) -> partialCompTbl)
    })
    compTbl
  }

  //generate a complete intersection table for all possible relations
  def preComputeIntersectTbl(inputRels:Set[BitSet],calculus:Calculus): HashMap[Int,HashMap[Int,Int]] ={
    var intersectTbl = HashMap[Int,HashMap[Int,Int]]()
    inputRels.foreach(rel1=>{
      var partialIntersectTbl = HashMap[Int,Int]()
      inputRels.foreach(rel2=>{
        val intersectRel = rel1 & rel2
        partialIntersectTbl += (calculus.setToIntLookUp(rel2) ->calculus.setToIntLookUp(intersectRel))
      })
      intersectTbl += (calculus.setToIntLookUp(rel1) -> partialIntersectTbl)
    })
    intersectTbl
  }

  //generate a size of relation table for all possible relations
  def preComputeSizeTbl(inputRels:Set[BitSet], calculus:Calculus): HashMap[Int,Int] ={
    var relSizeTbl = HashMap[Int,Int]()
    inputRels.foreach(rel1=>{
      relSizeTbl += (calculus.setToIntLookUp(rel1) -> rel1.size)
    })
    relSizeTbl
  }
}
