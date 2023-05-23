package queryengine.quanquery

import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import queryengine.helpers.TimeHelper
//import quanqueryengine.QuanQuery.{geoms, spark}
import queryengine.sparql.{ConvertTriplesToSQL, QueryParser}

object QuanQuery {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Query Engine")
      .config("spark.master", "local[4]")
      .config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
      .getOrCreate() // org.apache.sedona.core.serde.SedonaKryoRegistrator
    SedonaSQLRegistrator.registerAll(spark)

   // val config = spark.sparkContext.textFile("./example/quan-config.conf")
    val geometriesDir= args(0)
    val predicates = spark.sparkContext.textFile(args(1)).collect()
    val yagoDir= args(2)
    val queriesPath = args(3)
    val partitions = args(4).toInt

    // initialise the quan query executor
    //QuanQuery.init(config("geometries"))
    val geoms = spark.read.parquet(geometriesDir)
    geoms.createOrReplaceTempView("geoms")
    geoms
      .repartition(partitions)
      .cache()

    println("Geoms count:"+geoms.count)



    predicates.foreach(p => {
      val parts = p.split("/")
      val df = spark.read.parquet(yagoDir + parts(0) + "/" + parts(1))
      df.createOrReplaceTempView(parts(1))
      //df.cache()
      //println(parts(1)+":"+df.count())
    })

    val queries = spark
      .sparkContext
      .textFile(queriesPath)
      .collect()
      .mkString("\n")
      .split("-----")

    var qCount = 1

    queries.foreach(queryStr=> {
      TimeHelper.time {
        //println(queryStr)
        val queryParser = new QueryParser(queryStr)

        //result vars are the columns we want to select SELECT ?state ?actor
        val resultVars = queryParser.getResultVars()

        //do the non spatial part of the query
        val nonSpatialTriples = queryParser.getNonSpatialTriples()
        val queriesArr = ConvertTriplesToSQL.convert(nonSpatialTriples)

        var results = Map[String, DataFrame]()
        queriesArr.foreach(query => {
          val sqlDF = spark.sql(query).distinct()
            .cache()
          //for each variable map to the df of results e.g. m -> df that features m
          val columns = sqlDF.columns.toSeq
          columns.foreach(c => results += (c -> sqlDF))
        })

        for ((k, v) <- results) {
          println("key: " + k)
          println("count:"+results(k).count)
          results(k).show(300,false)
        }

//        ? p rdf : type yago : Nuclear_power_plant .
//          ? c rdf : type schema : Country .
//          ? p geo : sfWithin ? c

        //now the spatial part of the query
        val spatialTriple = queryParser.getSpatialTriple()

//        println("Spatial triples:")
//        println(spatialTriple)

        val rhsSpatialObjects = if (spatialTriple._3.indexOf("?") > -1) {
          val rhsSpatialObjectVar = spatialTriple._3.substring(1)
          results(rhsSpatialObjectVar)
            .select(col(rhsSpatialObjectVar).as("rhsSpatialObjectVar"))
            .distinct.collect.map(x => x(0).toString())

        } else {
          Array(spatialTriple._3)
        }

        val lhsSpatialObjects = if (spatialTriple._1.indexOf("?") > -1) {
          val lhsSpatialObjectVar = spatialTriple._1.substring(1)
          results(lhsSpatialObjectVar)
            .select(col(lhsSpatialObjectVar).as("lhsSpatialObjectVar"))
            .distinct.collect.map(x => x(0).toString())
        } else {
          Array(spatialTriple._1)
        }
        val rel = spatialTriple._2

        val quanResult = if (rel == "sfTouches" || rel == "sf-touches") {
          QuanQueryFncs.adjacency(spark, geoms, rhsSpatialObjects, lhsSpatialObjects)
        } else {
          QuanQueryFncs.containment(spark, geoms, rhsSpatialObjects, lhsSpatialObjects)
        }


        val spatialResult = if (spatialTriple._3.indexOf("?") > -1) {
          //it's a join
          val rhsSpatialObjectVar = spatialTriple._3.substring(1)
          val lhsSpatialObjectVar = spatialTriple._1.substring(1)
          quanResult
            .join(results(rhsSpatialObjectVar), col("parent") === col(rhsSpatialObjectVar))
            .drop(col("parent"))
            .join(results(lhsSpatialObjectVar), col("child") === col(lhsSpatialObjectVar))
            .drop("child")
        } else {
          val queryVar = spatialTriple._1.substring(1)
          quanResult
            .join(results(queryVar), col("child") === col(queryVar))
            .drop("child")
            .drop("parent")
        }

        spatialResult.show(false)

        spatialResult.createOrReplaceTempView("finalresults")
        val resultVarsStr = resultVars.reduce((a, b) => {
          a + "," + b
        })

        println(resultVarsStr)

        val finalResult = spark.sql("SELECT " + resultVarsStr + " FROM finalresults").cache()
        println("Final results:" + finalResult.count)
        finalResult.show(300,false)
      }
    })


  }
}
