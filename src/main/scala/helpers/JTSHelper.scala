package helpers

import com.google.common.geometry.{S2Cell, S2CellId, S2LatLng}
import com.vividsolutions.jts.geom._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable



object JTSHelper {

  def checkValidity(loops:mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Double]]]):Boolean={
    val polygon = convertPolygonToJTS(loops)
    return polygon.isValid()
  }

  def buildLinearRing(loop:mutable.WrappedArray[mutable.WrappedArray[Double]]):LinearRing={
    val geometryFactory = new GeometryFactory();
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
  def convertPolygonToJTS(loops:mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Double]]]): Polygon =
  {
    //println("loops size"+loops.size)
    val geometryFactory = new GeometryFactory()
    val shell = buildLinearRing(loops(0))
    val holes = ArrayBuffer[LinearRing]()
    for(j<-1 until loops.size){
      holes += buildLinearRing(loops(j))
    }
    //println("holes size"+holes.size)
    geometryFactory.createPolygon(shell,holes.toArray)
  }
  def convertMultiPolygonToJTS(polys:mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Double]]]]): MultiPolygon =
  {
    val geometryFactory = new GeometryFactory()
    val jtsPolys = ArrayBuffer[Polygon]()
    for(i<-0 until polys.size){
      jtsPolys += convertPolygonToJTS(polys(i))
    }

    geometryFactory.createMultiPolygon(jtsPolys.toArray)
  }

  def convertJTSCoordsToArray(coords:Array[Coordinate]):Array[Array[Double]]={
    val newCoords = coords.map(coord=>{
      Array(coord.y,coord.x)
    })

    newCoords
  }

  def convertJTSPolygonToArrayBuffer(jtsPolygon:Polygon):ArrayBuffer[Array[Array[Double]]]={
    val polyCoords = new ArrayBuffer[Array[Array[Double]]]()
    val coords = jtsPolygon.getExteriorRing().getCoordinates()
//    println("Outer shell")
    polyCoords += convertJTSCoordsToArray(coords)

    val holeCount = jtsPolygon.getNumInteriorRing()
//    println("hole count:"+holeCount)
    for(i<-0 until holeCount){
//      val coords = jtsPolygon.getInteriorRingN(i).getCoordinates()
      val linearRing = jtsPolygon.getInteriorRingN(i)

//      println("inner ring:"+i+" num points:"+linearRing.getNumPoints)
      val coords = linearRing.getCoordinates()
      polyCoords += convertJTSCoordsToArray(coords)
    }
//    println(polyCoords.map(ring=>ring.map(coords=>coords.mkString(",")).mkString("],[")).mkString("],\n["))
    polyCoords
  }



  def convertS2CellToCoordsArray(s2CellId:S2CellId) : ArrayBuffer[Coordinate]= {
    val s2Cell = new S2Cell(s2CellId)
    val cellCoordsArray = ArrayBuffer[Coordinate]()
    //verticies are returned in a CCW order
    val p0LatLng = new S2LatLng(s2Cell.getVertex(0))
    cellCoordsArray += new Coordinate(p0LatLng.lngDegrees(), p0LatLng.latDegrees())
    val p1LatLng = new S2LatLng(s2Cell.getVertex(1))
    cellCoordsArray += new Coordinate(p1LatLng.lngDegrees(), p1LatLng.latDegrees())
    val p2LatLng = new S2LatLng(s2Cell.getVertex(2))
    cellCoordsArray += new Coordinate(p2LatLng.lngDegrees(), p2LatLng.latDegrees())
    val p3LatLng = new S2LatLng(s2Cell.getVertex(3))
    cellCoordsArray += new Coordinate(p3LatLng.lngDegrees(), p3LatLng.latDegrees())
    cellCoordsArray += new Coordinate(p0LatLng.lngDegrees(), p0LatLng.latDegrees())
    cellCoordsArray
  }

  def getCellIntersection(coords: mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Double]]],
                          s2CellId:S2CellId) : ArrayBuffer[ArrayBuffer[Array[Array[Double]]]]=
  {
//    println("\n\nGetting new cell intersection")
    val s2Cell = new S2Cell(s2CellId)
    val jtsPoly = convertPolygonToJTS(coords)
    val cellCoordsArray = ArrayBuffer[Coordinate]()
    //vertices are returned in a CCW order
    val p0LatLng = new S2LatLng(s2Cell.getVertex(0))
    cellCoordsArray += new Coordinate(p0LatLng.lngDegrees(),p0LatLng.latDegrees())
    val p1LatLng = new S2LatLng(s2Cell.getVertex(1))
    cellCoordsArray += new Coordinate(p1LatLng.lngDegrees(),p1LatLng.latDegrees())
    val p2LatLng = new S2LatLng(s2Cell.getVertex(2))
    cellCoordsArray += new Coordinate(p2LatLng.lngDegrees(),p2LatLng.latDegrees())
    val p3LatLng = new S2LatLng(s2Cell.getVertex(3))
    cellCoordsArray += new Coordinate(p3LatLng.lngDegrees(),p3LatLng.latDegrees())
    cellCoordsArray += new Coordinate(p0LatLng.lngDegrees(),p0LatLng.latDegrees())

//    println("Top-left:"+p0LatLng.lngDegrees()+","+p0LatLng.latDegrees())
//    println("Top-right:"+p1LatLng.lngDegrees()+","+p1LatLng.latDegrees())
//    println("Bottom-right:"+p2LatLng.lngDegrees()+","+p2LatLng.latDegrees())
//    println("Bottom-left:"+p3LatLng.lngDegrees()+","+p3LatLng.latDegrees())
//
//    println("[")
//    println("["+p0LatLng.lngDegrees()+","+p0LatLng.latDegrees()+"]")
//    println("["+p1LatLng.lngDegrees()+","+p1LatLng.latDegrees()+"]")
//    println("["+p2LatLng.lngDegrees()+","+p2LatLng.latDegrees()+"]")
//    println("["+p3LatLng.lngDegrees()+","+p3LatLng.latDegrees()+"]")
//    println("]")


    val geometryFactory = new GeometryFactory()

    val cell = geometryFactory.createPolygon(cellCoordsArray.toArray)

    val intersectionGeometry = jtsPoly.intersection(cell)

//    println("Intersection valid:"+intersectionGeometry.isValid())
//    println("Num geometries:"+intersectionGeometry.getNumGeometries)

    if(intersectionGeometry.getNumPoints()==0){
      //could also use isEmpty
      //there is nothing in the intersection
    }
    val multiPolyCoords = new ArrayBuffer[ArrayBuffer[Array[Array[Double]]]]()
    if(intersectionGeometry.getGeometryType()=="Polygon"){
      //it's a polygon
      val jtsPoly = intersectionGeometry.asInstanceOf[Polygon]
      val polyCoords = convertJTSPolygonToArrayBuffer(jtsPoly)
      multiPolyCoords += polyCoords
    }
    if(intersectionGeometry.getGeometryType()=="MultiPolygon"){
//      println("Multi-polygon")
      //it's a multipolygon
      val jtsMultiPoly = intersectionGeometry.asInstanceOf[MultiPolygon]
      val geomCount = jtsMultiPoly.getNumGeometries()
//      println("geom count:"+geomCount)
      for(i<-0 until geomCount){
        val jtsPoly = jtsMultiPoly.getGeometryN(i).asInstanceOf[Polygon]
//        println("\n"+jtsPoly.toString)
//        println(jtsPoly.getNumPoints)
//        println(jtsPoly.isValid)

        val polyCoords = convertJTSPolygonToArrayBuffer(jtsPoly)
        multiPolyCoords += polyCoords
      }
    }
//    println("Multipolygon:")
//    println("[\n[\n[\n["+multiPolyCoords.map(poly=>poly.map(ring=>ring.map(coords=>coords.mkString(",")).mkString("],[")).mkString("]\n],\n[\n[")).mkString("]],\n[[")+"]\n]\n]\n]")

    multiPolyCoords
  }
  def getMultiPolygonCellIntersection(
        coords: mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[mutable.WrappedArray[Double]]]],
        s2CellId:S2CellId) : ArrayBuffer[ArrayBuffer[Array[Array[Double]]]]=
  {
    val s2Cell = new S2Cell(s2CellId)
    val jtsPoly = convertMultiPolygonToJTS(coords)
    val cellCoordsArray = ArrayBuffer[Coordinate]()
    //verticies are returned in a CCW order
    val p0LatLng = new S2LatLng(s2Cell.getVertex(0))
    cellCoordsArray += new Coordinate(p0LatLng.lngDegrees(),p0LatLng.latDegrees())
    val p1LatLng = new S2LatLng(s2Cell.getVertex(1))
    cellCoordsArray += new Coordinate(p1LatLng.lngDegrees(),p1LatLng.latDegrees())
    val p2LatLng = new S2LatLng(s2Cell.getVertex(2))
    cellCoordsArray += new Coordinate(p2LatLng.lngDegrees(),p2LatLng.latDegrees())
    val p3LatLng = new S2LatLng(s2Cell.getVertex(3))
    cellCoordsArray += new Coordinate(p3LatLng.lngDegrees(),p3LatLng.latDegrees())
    cellCoordsArray += new Coordinate(p0LatLng.lngDegrees(),p0LatLng.latDegrees())
    val geometryFactory = new GeometryFactory()
    val cell = geometryFactory.createPolygon(cellCoordsArray.toArray)
    val intersectionGeometry = jtsPoly.intersection(cell)

    if(intersectionGeometry.getNumPoints()==0){
      //could also use isEmpty
      //there is nothing in the intersection
    }
    val multiPolyCoords = new ArrayBuffer[ArrayBuffer[Array[Array[Double]]]]()
    if(intersectionGeometry.getGeometryType()=="Polygon"){
      //it's a polygon
      val jtsPoly = intersectionGeometry.asInstanceOf[Polygon]
      val polyCoords = convertJTSPolygonToArrayBuffer(jtsPoly)
      multiPolyCoords += polyCoords
    }
    if(intersectionGeometry.getGeometryType()=="MultiPolygon"){
      //it's a multipolygon
      val jtsMultiPoly = intersectionGeometry.asInstanceOf[MultiPolygon]
      val geomCount = jtsMultiPoly.getNumGeometries()
      for(i<-0 until geomCount){
        val jtsPoly = jtsMultiPoly.getGeometryN(i).asInstanceOf[Polygon]
        val polyCoords = convertJTSPolygonToArrayBuffer(jtsPoly)
        multiPolyCoords += polyCoords
      }
    }
    multiPolyCoords
  }


}
