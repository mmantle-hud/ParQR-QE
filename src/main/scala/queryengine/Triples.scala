package queryengine

import scala.collection.mutable.ArrayBuffer

class Triples {
  private var triples= new ArrayBuffer[(String,String,String)]()
  def addTriple(t:(String,String,String)): Unit ={
    triples+=t
  }
  def getTriples(): ArrayBuffer[(String,String,String)] ={
    triples
  }
  def clearTriples()={
    triples= new ArrayBuffer[(String,String,String)]()
  }

}
object Triples {
  private val _instance = new Triples()
  def instance()={
    _instance
  }
}

