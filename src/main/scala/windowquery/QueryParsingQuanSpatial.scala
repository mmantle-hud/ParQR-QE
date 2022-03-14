package windowquanfirst

import java.util

import com.esri.core.geometry._
import com.google.common.geometry.{S2CellId, S2RegionCoverer}
import helpers.{EsriHelper, S2Helper}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.collection.mutable.{Map, WrappedArray}



object QueryParsingQuanSpatial {
  def getCandidates(
                     spark: SparkSession,
                     queryTriple: (String, String, String),
                     nonSpatialResults: Map[String, DataFrame],
                     spatialIndex: DataFrame,
                     geoms: DataFrame
                   ): (Array[Long],DataFrame) = {

    import spark.implicits._


    val queryPolygonStr = queryTriple._3.substring(1, queryTriple._3.lastIndexOf("\""))

    //convert to ESRI Polygon
    val esriPolygon = OperatorImportFromWkt.local().execute(
      WktImportFlags.wktImportDefaults,
      Geometry.Type.Polygon,
      queryPolygonStr,
      null
    ).asInstanceOf[Polygon]
    val coords = EsriHelper.convertESRIPolygonToArray(esriPolygon)

    //convertToS2
    val S2Polygon = S2Helper.convertToS2(coords)

    //get the cells
    val regionCoverer = new S2RegionCoverer()
    regionCoverer.setMinLevel(9)
    regionCoverer.setMaxLevel(9)
    val initialCellIds = new util.ArrayList[S2CellId]()
    val covering = regionCoverer.getCovering(S2Polygon)
    covering.denormalize(9, 1, initialCellIds)

    val windowTokens = initialCellIds.toArray().map(cellId => {
      val s2CellId = cellId.asInstanceOf[S2CellId]
      val token = s2CellId.toToken()
      token
    })


    // println(queryTriple)
    val rel = queryTriple._2
    val queryVar = queryTriple._1.substring(1)


    //filter the quan index using cells from the window
    val filteredIndex = spatialIndex
      .filter($"token".isin(windowTokens: _*))
      .cache()

    //get the cells for these candidates
    val cells = filteredIndex
      .select($"cell")
      .rdd.map(x=>x(0).toString.toLong).distinct().collect()


    //we only want to keep candidates where all parts overlap the window
    val candidateCount = filteredIndex
      .select($"id", $"fullid", $"count")
      .distinct()
      .withColumn("num", lit(1))
      .groupBy($"id")
      .agg(
        sum($"num").as("total"),
        first($"count").as("count")
      )

    val candidates = candidateCount.filter($"total" === $"count")
      .select($"id")
      //.rdd.map(x=>x(0).toString).collect()


    (cells,candidates)
  }

  def filterCandidates(
                        spark:SparkSession,
                        queryTriple:(String,String,String),
                        nonSpatialResults:Map[String, DataFrame],
                        initResults:DataFrame,
                        cells:Array[Long],
                        geoms:DataFrame
                      ):DataFrame= {
    import spark.implicits._

    val queryPolygonStr = queryTriple._3.substring(1, queryTriple._3.lastIndexOf("\""))

    //convert to ESRI
    val esriQueryPolygon = OperatorImportFromWkt.local().execute(
      WktImportFlags.wktImportDefaults,
      Geometry.Type.Polygon,
      queryPolygonStr,
      null
    ).asInstanceOf[Polygon]

    val queryVar = queryTriple._1.substring(1)
    val nonSpatialObjs = nonSpatialResults(queryVar).select(col(queryVar))

    val candidateGeoms = geoms
      .filter($"cell".isin(cells:_*))
      .groupBy($"fullid")
      .agg(
        first($"id").as("id"),
        first($"coords").as("coords"),
        first($"geomtype").as("geomtype"),
        first($"count").as("count")
      )
      .select($"id",$"fullid",$"coords",$"count",$"geomtype")
      .cache()

    println("Candidate geoms count:"+candidateGeoms.count)

    candidateGeoms.select($"id").show(false)

    val checkGeoms = udf((coords:WrappedArray[WrappedArray[WrappedArray[Double]]], geomtype:String)=>{

      val matches = if (geomtype == "point") {
        val lat = coords(0)(0)(0)
        val lng = coords(0)(0)(1)
        val pt = new Point(lng, lat)
        OperatorContains.local().execute(esriQueryPolygon, pt, null, null)
      } else {
        val poly = EsriHelper.createPolyNoSR(coords)
        OperatorContains.local().execute(esriQueryPolygon, poly,null, null)
      }
      matches
    })

    val spatialResults = initResults.as("i")
      .join(candidateGeoms.as("g"),$"i.id"===$"g.id")
      .select(
        $"g.id".as("id"),
        $"g.fullid".as("fullid"),
        $"g.coords".as("coords"),
        $"g.geomtype".as("geomtype"),
        $"g.count".as("count")
      )
      .filter(checkGeoms($"coords",$"geomtype"))
      .select($"id",$"fullid",$"count")
      .distinct()
      .withColumn("num",lit(1))
      .groupBy($"id")
      .agg(
        sum($"num").as("total"),
        first($"count").as("count")
      )
      .filter($"total"===$"count")
      .cache()

    val resultCount = spatialResults.count
    println("Quan spatial results:"+resultCount)
    spatialResults.show(false)

    spatialResults.select($"id")
  }
}




