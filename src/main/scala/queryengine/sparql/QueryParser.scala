package queryengine.sparql

import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.Algebra

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.asScalaBufferConverter

class QueryParser(queryStr:String){
  org.apache.jena.query.ARQ.init()
  private val query = QueryFactory.create(queryStr)
  private val prefixes = query.getPrefixMapping()
  private val opRoot = Algebra.compile(query);
  private val queryVisitor = new QueryVisitor()
  opRoot.visit(queryVisitor)
  private var triples = queryVisitor.getTriples()


  def getTriples():ArrayBuffer[(String,String,String)] = {
    this.triples
  }

  def getNonSpatialTriples():ArrayBuffer[(String,String,String)] = {
    val nonSpatialTriples = this.triples.filter(x => {
      !(x._2 == "sf-touches" || x._2 == "sf-contains" || x._2 == "sfWithin" || x._2 == "sfTouches" || x._2 == "geosparql#sfWithin" || x._2 == "within")
    })
    nonSpatialTriples
  }

  def getSpatialTriple():(String,String,String) = {
    val spatialTriples = this.triples.filter(x => {
      x._2 == "sf-touches" || x._2 == "sf-contains" || x._2 == "sfWithin" || x._2 == "sfTouches" || x._2 == "within"
    })
    spatialTriples(0)
  }
  def getResultVars():Array[String] = {
    query.getResultVars().asScala.toArray
  }

}
