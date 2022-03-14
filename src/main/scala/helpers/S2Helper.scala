package helpers

import com.google.common.geometry.{S2LatLng, S2Polygon, S2PolygonBuilder}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Matthew on 09/07/2020.
  */
object S2Helper {
  def convertToS2(loops:ArrayBuffer[Array[Array[Double]]]): S2Polygon =
  {
    val polygonBuilder = new S2PolygonBuilder()
    for (i <- 0 until loops.length) {
      val loop = loops(i)
      val lat = loop(0)(0)
      val lng = loop(0)(1)
      for(j<-1 until loop.length){
        val point1 = S2LatLng.fromDegrees(loop(j-1)(0),loop(j-1)(1)).toPoint()
        val point2 = S2LatLng.fromDegrees(loop(j)(0),loop(j)(1)).toPoint()
        polygonBuilder.addEdge(point1, point2)
      }
      val point1 = S2LatLng.fromDegrees(loop(loop.length-1)(0),loop(loop.length-1)(1)).toPoint()
      val point2 = S2LatLng.fromDegrees(loop(0)(0),loop(0)(1)).toPoint()
      polygonBuilder.addEdge(point1, point2)
    }
    val polygon = polygonBuilder.assemblePolygon()
    polygon
  }
}
