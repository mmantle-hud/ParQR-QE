package queryengine.sparql

import scala.collection.mutable.ArrayBuffer

object ConvertTriplesToSQL {

  def getVars(triples:ArrayBuffer[(String,String,String)]):ArrayBuffer[String]  ={
    //get a list of all the variables
    val vars = triples.flatMap(triple => {
      Array(triple._1, triple._3)
    }).filter(v => {
      v.indexOf("?") > -1
    }).distinct
    vars
  }

  def getTriplesThatContainVar(triples: ArrayBuffer[(String, String, String)],
                               theVar: String
                              ): ArrayBuffer[(String, String, String)] = {
    triples.filter(triple => {
      (triple._1 == theVar || triple._3 == theVar)
    })
  }


  def updateTableNames(triples:ArrayBuffer[(String,String,String)]):ArrayBuffer[(String,String,String)]={
    var tables = collection.mutable.Map[String, Int]()
    //println("Updating triples to specify unique table names")
    val updatedTriples = triples.map(triple => {
      val mapVal = if (tables.contains(triple._2)) {
        tables(triple._2) + 1
      } else {
        1
      }
      tables(triple._2) = mapVal
      (triple._1, triple._2 + mapVal, triple._3)
    })
    updatedTriples
  }

  def convert(incomingtriples:ArrayBuffer[(String, String, String)]):ArrayBuffer[String]={
    var triples = updateTableNames(incomingtriples)
    val vars = getVars(triples)
    var usedVars = ArrayBuffer[String]()
    val selectStrArr = new ArrayBuffer[String]
    val sqlStrArr = new ArrayBuffer[String]
    val whereStrArr = new ArrayBuffer[String]
    val currentStmt = 0
    var selectStr = ""
    var sqlStr = ""
    var whereStr = ""

    //println("For each variable")
    vars.foreach(v => {
      //println("Considering: " + v)
      //println("Getting hold of all the triples that feature" + v)
      val varTriples = getTriplesThatContainVar(triples, v)
      //println("Getting hold of all the triples that feature" + v + " and haven't been used yet")
      var unusedVarTriples = varTriples.filter(t => !(usedVars.contains(t._1) || usedVars.contains(t._3)))

      //println("Getting hold of all the triples that feature" + v + " and have been used")
      var usedVarTriples = varTriples.filter(t => (usedVars.contains(t._1) || usedVars.contains(t._3)))

      //println("Which table are we joining to")
      val baseTriple = if (usedVarTriples.size > 0) {
        //       println(v + " Has already been used in so there is a table" +
        //         " that forms parts of the SQL statement that features " + v + "" +
        //         " so we take the first triple from the used triples, " +
        //         "knowing we can join using this")
        usedVarTriples(0)
      } else {
        //       println(v + " Hasn't already been used in so we need to start " +
        //         "from a table in the unused triples")
        unusedVarTriples(0)
      }
      val firstTable = baseTriple._2
      //println("The first table we will use is" + firstTable)
      val pos = if (baseTriple._1 == v) "subject" else "object"

      if (unusedVarTriples.size == varTriples.size) {
        //println("This variable hasn't been at all used previously" +
        // "\n we need to set up a new SQL statement")
        if (sqlStr.length > 0) {
          //println("if it exists, save the existing query")
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
      // println("Add the name of the table and variable to the" +
      //  "SELECT part of the query")
      selectStr += firstTable + "." + pos + " AS " + v.substring(1) + ", "
      if (!baseTriple._1.contains("?")) {
        //it's a WHERE with position 1 having the value e.g. <USA>
        //       whereStr += " AND " + baseTriple._2 + ".subject = " + baseTriple._1
        whereStr += " AND " + baseTriple._2 + ".subject = \"" + baseTriple._1 + "\""
      }
      if (!baseTriple._3.contains("?")) {
        //it's a WHERE with position 1 having the value e.g. <USA>
        whereStr += " AND " + baseTriple._2 + ".object = \"" + baseTriple._3 + "\""
      }


      //println("Add the JOIN to the main part of the query")
      unusedVarTriples.foreach(t => {
        val pos2 = if (t._1 == v) {
          "subject"
        } else {
          "object"
        }
        sqlStr += " \nJOIN " + t._2.substring(0, t._2.length - 1) + " AS " + t._2 + " ON " + firstTable + "." + pos + " = " + t._2 + "." + pos2
        //as well as joining we need to check if we have a WHERE clause
        if (!t._1.contains("?")) {
          //it's a WHERE with position 1 having the value e.g. <USA>
          //         whereStr += " AND " + t._2 + ".subject = " + t._1
          whereStr += " AND " + t._2 + ".subject = \"" + t._1 + "\""
        }
        if (!t._3.contains("?")) {
          //it's a WHERE with position 1 having the value e.g. <USA>
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
    var queriesArr = ArrayBuffer[String]()

    for (i <- 0 to selectStrArr.size - 1) {
      val q = selectStrArr(i) + " " + sqlStrArr(i) + "\n" + whereStrArr(i)
      // println("Going to run the following query")
      queriesArr += q
    }
    queriesArr

  }
}
