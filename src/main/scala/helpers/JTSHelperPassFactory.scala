package helpers

import com.google.common.geometry.{S2Cell, S2CellId, S2LatLng}
import com.vividsolutions.jts.geom._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by Matthew on 26/04/2021.
  */
object JTSHelperPassFactory {


  def buildLinearRing(
                       loop:mutable.WrappedArray[mutable.WrappedArray[Double]],
                       geometryFactory:GeometryFactory):LinearRing={
    val coordsArray = ArrayBuffer[Coordinate]()
    for (i <- 0 until loop.size) {
      val lat = loop(i)(0)
      val lng = loop(i)(1)
      //BigDecimal(1.23456789).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      //val lat = BigDecimal(loop(i)(0)).setScale(5, BigDecimal.RoundingMode.HALF_UP).toDouble
      //val lng = BigDecimal(loop(i)(1)).setScale(5, BigDecimal.RoundingMode.HALF_UP).toDouble
      coordsArray += new Coordinate(lng,lat)
    }
    val lat = loop(0)(0)
    val lng = loop(0)(1)
    coordsArray += new Coordinate(lng,lat)
    geometryFactory.createLinearRing(coordsArray.toArray)
  }
  def convertPolygonToJTS(
                           loops:mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Double]]],
                           geometryFactory:GeometryFactory
                         ): Polygon =
  {
    //println("loops size"+loops.size)
    val shell = buildLinearRing(loops(0), geometryFactory)
    val holes = ArrayBuffer[LinearRing]()
    for(j<-1 until loops.size){
      holes += buildLinearRing(loops(j),geometryFactory)
    }
    //println("holes size"+holes.size)
    geometryFactory.createPolygon(shell,holes.toArray)
  }


}
