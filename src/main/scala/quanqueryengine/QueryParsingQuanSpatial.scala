package spatialpartition.partitioning

import com.esri.core.geometry._
import com.vividsolutions.jts.geom.GeometryFactory
import helpers.EsriHelper
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable.{Map, WrappedArray}

object QueryParsingQuanSpatial {



  def runQuery(
                spark:SparkSession,
                queryTriple:(String,String,String),
                nonSpatialResults:Map[String, DataFrame],
                spatialIndex:DataFrame,
                geoms:DataFrame
              ):Map[String, DataFrame]= {

   import spark.implicits._

    println(queryTriple)
    val rel = queryTriple._2
    val queryVar = queryTriple._1.substring(1)
    var spatialObjectVar = queryTriple._3.substring(1)
    var spatialJoin = false
    val spatialObjectArr = if(queryTriple._3.indexOf("?") > -1){
      //it's a join, we need to use the existing results
      spatialJoin = true
      nonSpatialResults(spatialObjectVar)
        .select(col(spatialObjectVar).as("spatialObject"))
        .distinct.rdd.map(x=>x(0)).collect()
    }else{
      //it's not a join
      spatialObjectVar = queryTriple._3
      Array(queryTriple._3)
    }


    val spatialObjects = spatialIndex.filter($"id".isin(spatialObjectArr:_*))

    val nonSpatialObjs = nonSpatialResults(queryVar).select(col(queryVar))

    val nonSpatialIndex = spatialIndex
      .join(nonSpatialObjs,$"id" === col(queryVar)).select($"token",$"id",$"fullid",$"count")



    val initSpatialCandidates = spatialObjects.as("s")
      .join(nonSpatialIndex.as("n"),$"s.token"===$"n.token" && $"s.id"=!=$"n.id")
      .select(
        $"s.id".as("id_s"),
        $"s.fullid".as("fullid_s"),
        $"n.id".as("id_c"),
        $"n.fullid".as("fullid_c"),
        $"n.count".as("count"),
        $"s.cell".as("cell_g")
      )

      .cache()



    val spatialCandidates = if (rel == "sfWithin" || rel == "sf-within") {
       val candidatesAllParts =initSpatialCandidates
         .select($"id_s",$"id_c",$"fullid_c",$"count")
         .distinct()
         .withColumn("key",concat($"id_s",lit("#"),$"id_c"))
         .withColumn("num",lit(1))
         .groupBy($"key")
         .agg(
           sum($"num").as("total"),
           first($"count").as("count"),
           first($"id_s").as("id_s"),
           first($"id_c").as("id_c")
         )
         .filter($"total" === $"count")
         .select($"id_s".as("id_sk"),$"id_c".as("id_ck"))

        initSpatialCandidates
         .join(candidatesAllParts,$"id_c"===$"id_ck")
         .drop("id_sk","id_ck")
         .distinct()
         .cache()
    }else{
      initSpatialCandidates
        .distinct()
        .cache()
    }





    val cells = spatialCandidates
      .select($"cell_g")
      .rdd.map(x=>x(0).toString.toLong).distinct().collect()


    val candidateGeoms = geoms.filter($"cell".isin(cells:_*))
      .groupBy($"fullid")
      .agg(
        first($"coords").as("coords"),
        first($"geomtype").as("geomtype"),
        first($"count").as("count")
      )
      .select($"fullid",$"coords",$"count",$"geomtype")
      //.repartition()
      .cache()


    val checkGeoms = udf((
     coords1:WrappedArray[WrappedArray[WrappedArray[Double]]],
     type1:String,
     coords2:WrappedArray[WrappedArray[WrappedArray[Double]]],
     type2:String
                         )=>{
      val geometryFactory = new GeometryFactory()
      val matches = if (type1 == "point") {
        false //can't do geom computations with points
      } else {
        val check = if (type2 == "point") {
          //its a point
          val spatialObjectPoly = EsriHelper.createPolyNoSR(coords1)

          val lat = coords2(0)(0)(0)
          val lng = coords2(0)(0)(1)
          val pt = new Point(lng, lat)

          if (rel == "sfTouches" || rel == "sf-touches") {
            OperatorTouches.local().execute(spatialObjectPoly, pt, null, null)
          } else {
            OperatorContains.local().execute(spatialObjectPoly, pt, null, null)
          }

        } else {
          //it's a poly
          if (rel == "sfTouches" || rel == "sf-touches") {
            val poly1 = EsriHelper.createPolyNoSR(coords1)
            val poly2 = EsriHelper.createPolyNoSR(coords2)
            OperatorTouches.local().execute(poly1, poly2, null, null)
          } else {
            val poly1 = EsriHelper.createPolyNoSR(coords1)
            val poly2 = EsriHelper.createPolyNoSR(coords2)
            OperatorContains.local().execute(poly1, poly2,null, null)
          }
        }
        check
      }
      matches
    })



    val initSpatialResults = spatialCandidates
      .join(candidateGeoms.as("g1"),$"fullid_s"===$"g1.fullid")
      .join(candidateGeoms.as("g2"),$"fullid_c"===$"g2.fullid")
      .select(
        $"id_s",
        $"g1.fullid".as("fullid_s"),
        $"g1.coords".as("coords_s"),
        $"g1.geomtype".as("type_s"),
        $"id_c",
        $"g2.coords".as("coords_c"),
        $"g2.fullid".as("fullid_c"),
        $"g2.geomtype".as("type_c"),
        $"g2.count".as("count")
      )

      .filter(checkGeoms($"coords_s",$"type_s",$"coords_c",$"type_c"))
      .select($"id_s",$"id_c",$"fullid_c",$"count")
      .distinct()
      .cache()

    val resultCount = initSpatialResults.count
    println("Init Spatial results:"+resultCount)

    candidateGeoms.unpersist()




    val spatialResults = if(rel == "sfWithin" || rel == "sf-within"){

      val reducedResults = initSpatialResults
        .withColumn("key",concat($"id_s",lit("#"),$"id_c"))
        .withColumn("num",lit(1))
        .groupBy($"key")
        .agg(
          sum($"num").as("total"),
          first($"count").as("count"),
          first($"id_s").as("id_s"),
          first($"id_c").as("id_c")
        )
        .filter($"total" === $"count")
        .select($"id_s",$"id_c")
        .distinct()


      val spatialObjectsDF = spark.sparkContext
        .parallelize(spatialObjectArr)
        .map(x=>(x.toString,x.toString))
        .toDF("id_s","id_c")
      if(resultCount>0){
        spatialObjectsDF.union(reducedResults)
      }else{
        spatialObjectsDF
      }
    }else{
      initSpatialResults
        .select($"id_s",$"id_c")
        .distinct
    }



    if(spatialJoin){
      val join = spatialResults
        .join(nonSpatialResults(spatialObjectVar),$"id_s"===col(spatialObjectVar))
        .drop($"id_s")
        .join(nonSpatialResults(queryVar),$"id_c"===col(queryVar))
        .drop("id_c")

      join.columns.toSeq.foreach(col=>{
        nonSpatialResults(col) = join
      })

    }else{
      val join = spatialResults
        .join(nonSpatialResults(queryVar),$"id_c"===col(queryVar))
        .drop("id_c")
        .drop("id_s")

      join.columns.toSeq.foreach(col=>{
        nonSpatialResults(col) = join
      })

    }

    nonSpatialResults




  }



}
