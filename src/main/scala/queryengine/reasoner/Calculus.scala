package queryengine.reasoner

/**
  * Created by Matthew on 26/04/2021.
  */
import scala.collection.mutable.{BitSet, HashMap}

class Calculus(val basicRelationsStr:String,
               val inverseRelsStr:String,
               val compTblArr:Array[String]) {

  val basicRelationsArr = basicRelationsStr.split(" ").map(x=>x.toInt)
  val basicRelations = basicRelationsArr.toSet
  val basicRelSize = basicRelations.size
  val inverseRelsArr = inverseRelsStr.split(" ").map(x=>x.toInt)
  val inverseRels = this.getInverseRels(basicRelationsArr,inverseRelsArr)
  val basicCompTbl = this.getBasicCompTbl(basicRelations,compTblArr)
  val universalRel = (Math.pow(2,basicRelSize)-1).toInt
  val setToIntLookUp = this.getSetToIntLookUp(basicRelations)
  val intToSetLookUp = setToIntLookUp.map(x=>(x._2->x._1))

  //generate a look-up for inverse relations e.g. inverseRels(1)=8;
  private def getInverseRels(basicRelationsArr:Array[Int], inverseRelsArr:Array[Int]):HashMap[Int,Int]={
    var inverseRels=HashMap[Int,Int]()
    for(i <- 0 to basicRelationsArr.size-1){
      inverseRels += (basicRelationsArr(i)->inverseRelsArr(i))
    }
    inverseRels
  }

  //generate a look-up for the basic composition table e.g. basicCompTbl(1)(1)=1;
  private def getBasicCompTbl(basicRelations:Set[Int],compTblArr:Array[String]): HashMap[Int,HashMap[Int,BitSet]] ={
    val basicCompTbl=HashMap[Int,HashMap[Int,BitSet]]()
    //generate basic 'empty' composition table from the basic relations
    basicRelations.foreach(x=>{
      val hm=HashMap[Int,BitSet]()
      basicRelations.foreach(y=>{
        hm+=(y->BitSet(1))
      })
      basicCompTbl += (x->hm)
    })
    //populate it using config data
    compTblArr.map(x=>{
      val parts = x.split(" ")
      val rel=new BitSet()
      parts(2).split(",").foreach(x=>rel+=x.toInt)
      basicCompTbl(parts(0).toInt)(parts(1).toInt)=rel
    })
    basicCompTbl
  }

  //generate a Set to Int look-up for all possible relations e.g. BitSet(7,8)->3
  private def getSetToIntLookUp(basicRelations:Set[Int]):HashMap[BitSet, Int]={
    var setToIntLookUp =  HashMap[BitSet, Int]()
    val powerSetSize = Math.pow(2,basicRelations.size).toInt
    val basicRelsCount = basicRelations.size
    for (i <- 0 to powerSetSize-1){
      var bs = i.toBinaryString
      for(i <- bs.size to basicRelsCount-1){
        bs="0"+bs
      }
      var relSet = new BitSet()
      for(i<-0 to basicRelsCount-1){
        if(bs.charAt(i) == '1') relSet += (i + 1)
      }
      setToIntLookUp += (relSet->i)
    }
    setToIntLookUp
  }

  private def getInverseRelLookup(intToSetLookUp:HashMap[Int,BitSet],
                                  inverseRels:HashMap[Int,Int],
                                  setToIntLookUp:HashMap[BitSet, Int])
  : HashMap[Int,Int] ={
    intToSetLookUp.map(x=>{
      val invSet=BitSet()
      x._2.foreach(rel=>{
        invSet+=inverseRels(rel)
      })
      val invRel = setToIntLookUp(invSet)
      (x._1->invRel)
    })

  }

  def convertIntToSet(intRel:Int):Set[Int]={
    intToSetLookUp(intRel).toSet[Int]
  }
}
