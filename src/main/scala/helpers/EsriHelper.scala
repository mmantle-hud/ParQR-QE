package helpers

import com.esri.core.geometry.{OperatorSimplifyOGC, Polygon, SpatialReference}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Matthew on 16/06/2021.
  * libraryDependencies += "com.esri.geometry" % "esri-geometry-api" % "2.2.4"
  */
object EsriHelper {
//  def createPoly(loops:mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Double]]],sr:SpatialReference):Polygon={
//    val precision = 4
//    val poly = new Polygon()
//    for(i<-0 until loops.size){
//      val loop = loops(i)
//      val y = BigDecimal(loop(0)(0)).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
//      val x = BigDecimal(loop(0)(1)).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
//      poly.startPath(x,y)
//      for(j<-1 until loop.size){
//        val y = BigDecimal(loop(j)(0)).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
//        val x = BigDecimal(loop(j)(1)).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
//        poly.lineTo(x,y)
//      }
//    }
//
//    val g = OperatorSimplifyOGC.local().execute(poly, sr, true, null)
//    g.asInstanceOf[Polygon]
//  }
  def createPolyNoSR(loops:mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Double]]]):Polygon={
    val poly = new Polygon()
    val precision = 4
    for(i<-0 until loops.size){
      val loop = loops(i)
      val x = BigDecimal(loop(0)(0)).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
      val y = BigDecimal(loop(0)(1)).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
      poly.startPath(x,y)
      for(j<-1 until loop.size){
        val x = BigDecimal(loop(j)(0)).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
        val y = BigDecimal(loop(j)(1)).setScale(precision, BigDecimal.RoundingMode.HALF_UP).toDouble
        poly.lineTo(x,y)
      }
    }

    val g = OperatorSimplifyOGC.local().execute(poly, null, true, null)
    g.asInstanceOf[Polygon]
  }
  def convertESRIPolygonToArray(polygon:Polygon):ArrayBuffer[Array[Array[Double]]]={
    val coords = polygon.getCoordinates2D()
    val coordsArr = coords.map(pair=>{
      //This outputs (lat,lng) because it will be used by S2
      Array(pair.y,pair.x)
    })
    ArrayBuffer(coordsArr)
  }
}
