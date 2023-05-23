package kggenerator

import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, udf}

/*
Takes the quantitative spatial data i.e. the GADM region geometries and the YAGO points
and generates a single geometries table
 */
object GenerateQuanData {
  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("Generate Quan Data")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
      .getOrCreate()

    SedonaSQLRegistrator.registerAll(spark)
    import spark.implicits._

    val regionDir = args(0)
    val pointsDir = args(1)
    val matchesDir = args(2)
    val outputDir = args(3)


    //get the GADM geometries
    var regions = spark.read.parquet(regionDir+"/layer0").union(
      spark.read.parquet(regionDir+"/layer1")
    ).select(
      $"region_id".as("id"),
      $"geometry".as("geometry"),
    )

    //get the YAGO points
    val matches = spark.read.parquet(matchesDir+"/layer0").union(
      spark.read.parquet(matchesDir+"/layer1")
    )

    //swap in the matches
    def getNewValue = udf((currentVal:String,newVal:String)=>{
      val finalVal = if(newVal == null){
        currentVal
      }else{
        newVal
      }
      finalVal
    })

    regions = regions.as("r")
      .join(matches.as("m"),$"r.id" === $"m.region_id", "left_outer")
      .withColumn("new_subject",getNewValue($"r.id",$"m.subject"))
      .select($"new_subject".as("id"),$"geometry".as("geometry"))


    var points = spark.read.parquet(pointsDir).select(
      $"subject".as("id"),
      $"geometry".as("geometry"),
    )
    points.show(false)
    println(points.count)

    // we only want the points that haven't been matched to a region
    points = points.as("p")
      .join(regions.as("r"),$"p.id" === $"r.id", "left_anti")
      .select($"p.id".as("id"),$"p.geometry".as("geometry"))


    //need to generate a count for each geometry
    //this is for containment tests
    //need to be able check all the polygons of multi-polygon geoms are within the query window

    val geometriesNoCounts = regions.union(points)
    geometriesNoCounts.cache()
    geometriesNoCounts.show()

    //generate the count for each geometry
    val geomCounts = geometriesNoCounts
      .groupBy($"id")
      .agg(count($"id").as("count"))

    geomCounts.cache()


    //now join back to the full info to add a count value for each row
    val geometries = geometriesNoCounts.as("g")
      .join(geomCounts.as("c"),$"g.id" === $"c.id")
      .select(
        $"g.id".as("id"),
        $"c.count".as("count"),
        $"g.geometry".as("geometry")
      )

    geometries.show()
    geometries.write.mode("overwrite").parquet(outputDir)


  }
}
