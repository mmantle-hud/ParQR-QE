package queryengine.quanquery

import org.apache.spark.sql.functions.{col, count, desc, first}
import org.apache.spark.sql.{DataFrame, SparkSession}




object QuanQueryFncs {

  def containment(spark:SparkSession, geoms:DataFrame, spatialObjects:Array[String], queryObjects:Array[String])={

    import spark.implicits._

    val spatialGeometries = geoms.filter(col("id").isin(spatialObjects:_*))
    spatialGeometries.cache()
    println("Spatial geometries:"+spatialGeometries.count)
    spatialGeometries.select($"id",$"count").sort(desc("count")).show(50,false)

    spatialGeometries.createOrReplaceTempView("spatialGeometries")

    val queryGeometries = geoms.filter(col("id").isin(queryObjects:_*))
    queryGeometries.cache()
    println("Query geometries:"+queryGeometries.count)
    queryGeometries.select($"id",$"count").sort(desc("count")).show(50, false)
    queryGeometries.createOrReplaceTempView("queryGeometries")

    val resultsDf = spark.sql(
      "SELECT spatialGeometries.id as parent, queryGeometries.id as child, queryGeometries.count as childcount FROM spatialGeometries, queryGeometries " +
        "WHERE ST_Within(queryGeometries.geometry, spatialGeometries.geometry) OR queryGeometries.id = spatialGeometries.id"
    ).groupBy(col("child"))
      .agg(
        first("parent").as("parent"),
        count("child").as("querycount"),
        first("childCount").as("childcount")
      ).filter(col("queryCount") === col("childCount"))
      .select(col("parent"),col("child"))

    resultsDf

  }
 def adjacency(spark:SparkSession, geoms:DataFrame, spatialObjects:Array[String], queryObjects:Array[String])={

    import spark.implicits._

    val spatialGeometries = geoms.filter(col("id").isin(spatialObjects:_*))

    spatialGeometries.createOrReplaceTempView("spatialGeometries")

    val queryGeometries = geoms.filter(col("id").isin(queryObjects:_*))
    //println("Query geometries:")
    //queryGeometries.show()
    queryGeometries.createOrReplaceTempView("queryGeometries")

    val resultsDf = spark.sql(
      "SELECT spatialGeometries.id as parent, queryGeometries.id as child FROM spatialGeometries, queryGeometries " +
        "WHERE ST_Touches(queryGeometries.geometry, spatialGeometries.geometry) AND queryGeometries.id != spatialGeometries.id"
    ).distinct()

    resultsDf

  }

  def rangeQuery(spark:SparkSession, geoms:DataFrame, queryPolygon:String): DataFrame = {
    val queryWindow = spark.sql("SELECT ST_GeomFromWKT('"+queryPolygon+"') AS geometry")
    queryWindow.createOrReplaceTempView("queryWindow")

    geoms.createOrReplaceTempView("spatialGeometries")

    val resultsDf = spark.sql(
      "SELECT spatialGeometries.id as child, spatialGeometries.count as childcount FROM spatialGeometries, queryWindow " +
        "WHERE ST_Contains (queryWindow.geometry, spatialGeometries.geometry)"
    ).groupBy(col("child"))
      .agg(
        count("child").as("querycount"),
        first("childCount").as("childcount")
      )
      .filter(col("queryCount") === col("childCount"))
      .select(col("child"))

    resultsDf

  }


  //  def containment(spark:SparkSession, geoms:DataFrame, spatialObjects:Array[String], queryObjects:Array[String])={
  def noFilteringContainment(spark:SparkSession, geoms:DataFrame, spatialObjects:Array[String])={
    import spark.implicits._

    val spatialGeometries = geoms.filter(col("id").isin(spatialObjects:_*))

    //println("Spatial geometries:")
    //spatialGeometries.show()

    spatialGeometries.createOrReplaceTempView("spatialGeometries")

    val queryGeometries = geoms //.filter(col("id").isin(queryObjects:_*))
    //println("Query geometries:")
    //queryGeometries.show()
    queryGeometries.createOrReplaceTempView("queryGeometries")

    val resultsDf = spark.sql(
      "SELECT spatialGeometries.id as parent, queryGeometries.id as child, queryGeometries.count as childcount FROM spatialGeometries, queryGeometries " +
        "WHERE ST_Within(queryGeometries.geometry, spatialGeometries.geometry)"
    ).groupBy(col("child"))
      .agg(
        first("parent").as("parent"),
        count("child").as("querycount"),
        first("childCount").as("childcount")
      ).filter(col("queryCount") === col("childCount"))
      .select(col("parent"),col("child"))

    resultsDf

  }
  //  def adjacency(spark:SparkSession, geoms:DataFrame, spatialObjects:Array[String], queryObjects:Array[String])={
  def noFilteringAdjacency(spark:SparkSession, geoms:DataFrame, spatialObjects:Array[String])={
    import spark.implicits._

    val spatialGeometries = geoms.filter(col("id").isin(spatialObjects:_*))

    spatialGeometries.createOrReplaceTempView("spatialGeometries")

    val queryGeometries = geoms //.filter(col("id").isin(queryObjects:_*))
    //println("Query geometries:")
    //queryGeometries.show()
    queryGeometries.createOrReplaceTempView("queryGeometries")

    val resultsDf = spark.sql(
      "SELECT spatialGeometries.id as parent, queryGeometries.id as child FROM spatialGeometries, queryGeometries " +
        "WHERE ST_Touches(queryGeometries.geometry, spatialGeometries.geometry) AND queryGeometries.id != spatialGeometries.id"
    ).distinct()

    resultsDf

  }


}
