package queryengine

import org.apache.jena.query.QueryFactory
import org.apache.jena.sparql.algebra.Algebra
import org.apache.spark.sql.{DataFrame, SparkSession}
import collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Map}

object QueryParsingSQL {



  def getResultVars(queryString:String): Array[String] ={
    val query = QueryFactory.create(queryString)
    query.getResultVars().asScala.toArray
  }

  def getTriples(queryString:String):ArrayBuffer[(String,String,String)]={
    println("\nStarting a new query\n" + queryString)
    Triples.instance().clearTriples()


    val query = QueryFactory.create(queryString)

    val prefixes = query.getPrefixMapping()
    val opRoot = Algebra.compile(query)

    opRoot.visit(new AlgebraTransformer())


    val allTriples = Triples.instance().getTriples()


     allTriples.foreach(println)

    Triples.instance().getTriples()
  }

  def noJoinSpatialTriple(triples:ArrayBuffer[(String,String,String)]):Boolean = {
    val matchingTriples = triples.filter(t => !(t._1.indexOf("?") > -1 && t._3.indexOf("?") > -1))
    matchingTriples.nonEmpty
  }

  def joinSpatialTriple(triples:ArrayBuffer[(String,String,String)]):Boolean = {
    val matchingTriples = triples.filter(t => (t._1.indexOf("?") > -1 && t._3.indexOf("?") > -1))
    matchingTriples.nonEmpty
  }
  def getNonSpatialTriples(triples:ArrayBuffer[(String,String,String)]):ArrayBuffer[(String,String,String)] = {
    val nonSpatialTriples = triples.filter(x => {
      !(x._2 == "sf-touches" || x._2 == "sf-contains" || x._2 == "sfWithin" || x._2 == "sfTouches" || x._2 == "geosparql#sfWithin" || x._2 == "within")
    })
    nonSpatialTriples
  }

  def getSpatialTriples(triples:ArrayBuffer[(String,String,String)]):ArrayBuffer[(String,String,String)] = {
    val spatialTriples = triples.filter(x => {
      x._2 == "sf-touches" || x._2 == "sf-contains" || x._2 == "sfWithin" || x._2 == "sfTouches" || x._2 == "within"
    })

    spatialTriples

  }


 def getNonSpatialResults(
     spark:SparkSession,triples:ArrayBuffer[(String,String,String)]
                         ):Map[String, DataFrame]={
   var tables = collection.mutable.Map[String, Int]()


   val updatedTriples = triples.map(triple => {
     val mapVal = if (tables.contains(triple._2)) {
       tables(triple._2) + 1
     } else {
       1
     }
     tables(triple._2) = mapVal
     (triple._1, triple._2 + mapVal, triple._3)
   })

   val vars = updatedTriples.flatMap(triple => {
     Array(triple._1, triple._3)
   }).filter(v => {
     v.indexOf("?") > -1
   }).distinct

   var usedVars = ArrayBuffer[String]()


   def getTriplesThatContainVar(triples: ArrayBuffer[(String, String, String)],
                                theVar: String,
                                usedVars: ArrayBuffer[String]): ArrayBuffer[(String, String, String)] = {
     triples.filter(triple => {
       (triple._1 == theVar || triple._3 == theVar)
     })
   }

   val selectStrArr = new ArrayBuffer[String]
   val sqlStrArr = new ArrayBuffer[String]
   val whereStrArr = new ArrayBuffer[String]
   val currentStmt = 0
   var selectStr = ""
   var sqlStr = ""
   var whereStr = ""

   vars.foreach(v => {

     val varTriples = getTriplesThatContainVar(updatedTriples, v, usedVars)

     var unusedVarTriples = varTriples.filter(t => !(usedVars.contains(t._1) || usedVars.contains(t._3)))


     var usedVarTriples = varTriples.filter(t => (usedVars.contains(t._1) || usedVars.contains(t._3)))


     val baseTriple = if (usedVarTriples.size > 0) {

       usedVarTriples(0)
     } else {

       unusedVarTriples(0)
     }
     val firstTable = baseTriple._2

     val pos = if (baseTriple._1 == v) "subject" else "object"

     if (unusedVarTriples.size == varTriples.size) {

       if (sqlStr.length > 0) {

         selectStrArr += selectStr.substring(0, selectStr.length - 2)
         sqlStrArr += sqlStr
         if (whereStr.length > 0) {
           whereStrArr += "WHERE " + whereStr.substring(4)
         } else {
           whereStrArr += whereStr
         }
       }

       selectStr = "SELECT "
       sqlStr = " FROM " + firstTable.substring(0, firstTable.length - 1) + " AS " + firstTable
       whereStr = ""

       unusedVarTriples = unusedVarTriples.drop(1)
     }

     selectStr += firstTable + "." + pos + " AS " + v.substring(1) + ", "
     if (!baseTriple._1.contains("?")) {

       whereStr += " AND " + baseTriple._2 + ".subject = \"" + baseTriple._1 + "\""
     }
     if (!baseTriple._3.contains("?")) {

       whereStr += " AND " + baseTriple._2 + ".object = \"" + baseTriple._3 + "\""
     }



     unusedVarTriples.foreach(t => {
       val pos2 = if (t._1 == v) {
         "subject"
       } else {
         "object"
       }
       sqlStr += " \nJOIN " + t._2.substring(0, t._2.length - 1) + " AS " + t._2 + " ON " + firstTable + "." + pos + " = " + t._2 + "." + pos2

       if (!t._1.contains("?")) {

         whereStr += " AND " + t._2 + ".subject = \"" + t._1 + "\""
       }
       if (!t._3.contains("?")) {

         whereStr += " AND " + t._2 + ".object = \"" + t._3 + "\""
       }
     })
     usedVars += v
   }) //end of vars foreach loop

   selectStrArr += selectStr.substring(0, selectStr.length - 2)
   sqlStrArr += sqlStr
   if (whereStr.length > 0) {
     whereStrArr += "WHERE " + whereStr.substring(4)
   } else {
     whereStrArr += whereStr
   }


   var results = Map[String, DataFrame]()
   for (i <- 0 to selectStrArr.size - 1) {
     val q = selectStrArr(i) + " " + sqlStrArr(i) + "\n" + whereStrArr(i)

     println(q)

     val sqlDF = spark.sql(q).distinct()
       .cache()
     println("Non-spatial query results:"+sqlDF.count)

     val columns = sqlDF.columns.toSeq
     columns.foreach(c => results += (c -> sqlDF))

   }
   results
 }

}
