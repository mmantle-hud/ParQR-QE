package queryengine.hybridquery

import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import queryengine.helpers.TimeHelper
import queryengine.qualquery.QualQuery
import queryengine.reasoner.{Calculus, PreProcessFncs, QualitativeSpatialReasoner}
import queryengine.quanquery.{QuanQuery, QuanQueryFncs}
import queryengine.sparql.{ConvertTriplesToSQL, QueryParser}

object HybridQuery {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Query Engine")
      .config("spark.master", "local[4]")
      .config("spark.serializer", classOf[KryoSerializer].getName) // org.apache.spark.serializer.KryoSerializer
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
      .getOrCreate() // org.apache.sedona.core.serde.SedonaKryoRegistrator
    SedonaSQLRegistrator.registerAll(spark)


    val rcc8NetworkDir = args(0)
    val dictionaryDir = args(1)
    val calculusDir = args(2)
    val geomsDir = args(3)
    val predicates = spark.sparkContext.textFile(args(4)).collect()
    val yagoDir = args(5)
    val queriesPath = args(6)
    val queryType = args(7)
    val partitions = args(8).toInt


      val calculusRDD = spark.sparkContext.textFile(calculusDir).collect
      val calculus = new Calculus(calculusRDD(0), calculusRDD(1), calculusRDD.drop(2))

      //load the RCC8 network
      val inputTriples = spark.sparkContext.textFile(rcc8NetworkDir)
      //convert the input lines into tuples of the form (Int,BitSet,Int,Int)
      val inputRels = PreProcessFncs.convertInput(inputTriples)
      //get transpose of input network
      val inverseRels = PreProcessFncs.getTranspose(inputRels, calculus)
      //combine transpose with initial input
      val allInputRels = inputRels.union(inverseRels).distinct()

      val dictionary = spark.read.parquet(dictionaryDir).cache()
      println("Items in the dictionary:" + dictionary.count)

      //convert BitSet into Int for input dataset
      val setToIntlookUp = calculus.setToIntLookUp
      val universalRel = calculus.universalRel
      val allTriples = allInputRels.map(x => (x._1, setToIntlookUp(x._2), x._3, x._4)).cache()

      //get a list of input relations and generate possible results of composition  based on this
      val possibleRels = PreProcessFncs.generatePossibleRelations(allInputRels, calculus)
      //using this list of relations pre-compute the composition table
      val compTbl = PreProcessFncs.preComputeCompTbl(possibleRels, calculus)
      //the same but pre-compute the intersection table
      val intersectTbl = PreProcessFncs.preComputeIntersectTbl(possibleRels, calculus)
      //now generate a size of relation look-up table
      val relSizeTbl = PreProcessFncs.preComputeSizeTbl(possibleRels, calculus)

      println("Loaded an RCC8 network of size:" + allTriples.count())
      println("It has this many partitions:" + allTriples.getNumPartitions)

      val keyedTriples = allTriples
        .keyBy(_._1)
        .groupByKey
        .mapValues(_.toList)
        .collectAsMap

      val keyedTriplesB = spark.sparkContext.broadcast(keyedTriples)
      val compTblB = spark.sparkContext.broadcast(compTbl)
      val intersectTblB = spark.sparkContext.broadcast(intersectTbl)
      val relSizeTblB = spark.sparkContext.broadcast(relSizeTbl)

      println("Broadcast triple var size:" + keyedTriplesB.value.size)
      println("Broadcast Composition table var size:" + compTblB.value.size)
      println("Broadcast Intersection table var size:" + intersectTblB.value.size)
      println("Broadcast Relation size table var size:" + relSizeTblB.value.size)
      //end of qual init


    // initialise the quan query executor
    val geoms =  spark.read.parquet(geomsDir)
      geoms.createOrReplaceTempView("geoms")

    geoms.cache()

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

    queries.foreach(queryStr => {
      TimeHelper.time {
        //println(queryStr)
        val queryParser = new QueryParser(queryStr)

        //result vars are the columns we want to select SELECT ?state ?actor
        val resultVars = queryParser.getResultVars()

        println("triples:")
        queryParser.getTriples().foreach(println)


        //do the non spatial part of the query
        val nonSpatialTriples = queryParser.getNonSpatialTriples()

        println("Non spatial triples:")
        nonSpatialTriples.foreach(println)


        val queriesArr = ConvertTriplesToSQL.convert(nonSpatialTriples)

        var results = Map[String, DataFrame]()
        queriesArr.foreach(query => {
          val sqlDF = spark.sql(query).distinct()
            .cache()
          println("Non-spatial query results:"+sqlDF.count)
          //for each variable map to the df of results e.g. m -> df that features m
          val columns = sqlDF.columns.toSeq
          columns.foreach(c => results += (c -> sqlDF))
        })

        for ((k, v) <- results) {
          println("key: " + k)
          results(k).show(false)
        }

        //now the spatial part of the query
        val spatialTriple = queryParser.getSpatialTriple()

          //start of hybrid
          //first do quan reasoning
          val queryPolygon = spatialTriple._3.substring(1, spatialTriple._3.lastIndexOf("\""))

          println(queryPolygon)

          val quanResults = QuanQueryFncs.rangeQuery(spark, geoms, queryPolygon)

          println("Quan results:")
          println("Quan results:"+quanResults.count)
          quanResults.show(false)


          val spatialObjects = quanResults.collect.map(x => x(0).toString())

          val setRel = Set(6,7)
          val qualResults = QualQuery.executeQualQuery(
            spark,
            allTriples,
            keyedTriplesB,
            dictionary,
            compTblB,
            intersectTblB,
            relSizeTblB,
            calculus,
            spatialObjects,
            setRel,
            partitions
          )

        println("Qual results")
        qualResults.show(false)
          println("Qual results:")
          qualResults.show(false)

          val parents = qualResults.select(col("parent")).withColumnRenamed("child", "parent")
          val children = qualResults.select(col("child"))
          val spatialResults = quanResults.union(parents).union(children).distinct()
          //end of hybrid


        var lhsSpatialObjectVar = spatialTriple._1.substring(1)
        val join = spatialResults
          .join(results(lhsSpatialObjectVar), col("child") === col(lhsSpatialObjectVar))
          .drop("child")

        //join.show(false)
        join.createOrReplaceTempView("finalresults")
        val resultVarsStr = resultVars.reduce((a, b) => {
          a + "," + b
        })

        val finalResult = spark.sql("SELECT " + resultVarsStr + " FROM finalresults").cache()
        println("Final results:" + finalResult.count)
        finalResult.show(false)

      }
    })
  }


}
