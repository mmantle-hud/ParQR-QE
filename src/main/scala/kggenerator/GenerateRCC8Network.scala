package kggenerator


import org.apache.sedona.sql.utils.SedonaSQLRegistrator
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}


/*
Generates an RCC-8 network from the enhanced knowledge graph. This involves
Taking computed EC relations and computed point in polygon relations and mapping these to RCC-8 relations
Generating RCC-8 relations from GADM Ids and YAGO schema:containedInPlace relations
Substituting YAGO URIs for the GADM Ids for the discovered matches
*/

object GenerateRCC8Network {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("Generate RCC8 Network")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[SedonaVizKryoRegistrator].getName)
      .getOrCreate()

    SedonaSQLRegistrator.registerAll(spark)


    import spark.implicits._
    import org.apache.spark.sql.functions._

    val pointInPolygonDir = args(0)
    val pointsDir = args(1)
    val ecRelDir = args(2)
    val layer1Dir = args(3)
    val containedInPlaceDir = args(4)
    val matchesDir = args(5)
    val partitions = args(6).toInt
    val rcc8OutputDir = args(7)



    //Take the point in region results and map to NTPP relations
    var pointInRegion = spark
      .read
      .parquet(pointInPolygonDir)
      .withColumn("rel",lit("5,"))
      .withColumn("source",lit("quan"))
      .select($"subject".as("subject"),$"rel",$"region_id".as("object"),$"source")
    pointInRegion.show(false)

    //Take schema:containedInPlace relations and map to NTPP RCC-8 relation
    //To prevent inconsistencies, we generate a single relation for each YAGO resource
    var containedInPlace = spark.read
      .parquet(containedInPlaceDir)
      .groupBy($"subject")
      .agg(first($"object").as("object"))
      .withColumn("rel",lit("5,"))
      .withColumn("source",lit("qual"))
      .select($"subject",$"rel",$"object",$"source")
      //.filter($"subject" =!= "<http://yago-knowledge.org/resource/Sahrawi_Arab_Democratic_Republic>")


    println("Contained in place:"+containedInPlace.count)
    containedInPlace.show(false)


    // Generate TPP/NTPP relations for GADM regions using the parent region id
    var gadmNTTP = spark.read.parquet(layer1Dir)
      .select($"region_id".as("subject"))
      .distinct()
      .withColumn("rel",lit("4,5,"))
      .withColumn("source",lit("gadm"))
      .withColumn("object",substring($"subject",0,3))
      .select($"subject",$"rel",$"object",$"source")

    println("GADM NTTP count: "+gadmNTTP.count)
    gadmNTTP.show()

    //Generate RCC-8 EC relations from adjacency computations
    var ec = spark.read.parquet(ecRelDir+"layer0")
      .union(spark.read.parquet(ecRelDir+"layer1"))
      .withColumn("rel",lit("2,"))
      .select($"region_id1".as("subject"),$"rel",$"region_id2".as("object"))

    println("EC count:"+ec.count)
    ec.show(false)

    def generateECKey = udf((id1:String,id2:String)=>{
      val key = if(id2 < id1){
        id2+"#"+id1
      }else{
        id1+"#"+id2
      }
      key
    })

    //remove duplicate EC relations
    ec = ec.withColumn("key",generateECKey($"subject",$"object"))
    ec = ec.groupBy($"key").agg(
      first($"subject").as("subject"),
      first($"rel").as("rel"),
      first($"object").as("object")
    ).cache()
    println("Grouped EC:"+ec.count)

    //now get hold of the matches
    val matches = spark.read.parquet(matchesDir+"layer0").union(spark.read.parquet(matchesDir+"layer1"))
    matches.cache()
    matches.show(false)


    println("Now doing the matches:")

    //collect all the matches
    val matchesList = matches
      .rdd
      .map(row=>(row(0).toString,row(1).toString))
      .collectAsMap()

    //Now swap the matches into the generated RCC-8 network
    def checkForMatch = udf((region_id:String)=>{
        if(matchesList.contains(region_id)){
          matchesList(region_id)
        }else{
          region_id
        }
    })

    pointInRegion = pointInRegion.withColumn("object",checkForMatch($"object"))
    ec = ec.withColumn("subject",checkForMatch($"subject"))
    ec = ec.withColumn("object",checkForMatch($"object"))
    gadmNTTP = gadmNTTP.withColumn("subject",checkForMatch($"subject"))
    gadmNTTP = gadmNTTP.withColumn("object",checkForMatch($"object"))

    pointInRegion.show(50,false)
    ec.show(false)
    gadmNTTP.show(false)


    //need remove points that are regions e.g. yago:Belgium 4,5 BEL
    val pointInRegion_regions_removed = pointInRegion.as("p")
      .join(matches.as("m"),$"p.subject" === $"m.subject", "left_anti")


    //also need to remove points from pointInRegion that matching GADM regions are within
    //e.g. containedInPlace yago:Germany 4,5 yago:Central Europe
    //pointInRegion yago:Central Europe 4,5, Hesse (this needs removing)
    val pointsToRemove = matches.as("m")
      .join(containedInPlace.as("c"),$"m.subject" === $"c.subject")
      .select($"c.object".as("object"))

    // now remove these from pointInRegion
    val pointInRegion_cleaned = pointInRegion_regions_removed.as("p")
      .join(pointsToRemove.as("r"),$"p.subject" === $"r.object", "left_anti")

    //also need to remove these from containedInPlace
    val containedInPlace_cleaned = containedInPlace.as("c")
      .join(pointsToRemove.as("r"),$"c.subject" === $"r.object", "left_anti")


    //now resolve conflicts between different sources
    //this is for containment relations only
    val containRels = pointInRegion_cleaned
      .union(containedInPlace_cleaned)
      .union(gadmNTTP)
      .rdd
      .map(row => (row(0),(row(0),row(1),row(2),row(3))) )
      .reduceByKey((a,b)=>{
        if(a._4 == "gadm"){
          a
        }else if (b._4 == "gadm"){
          b
        }else if(a._4 == "quan"){
          a
        }else if(b._4 == "quan"){
          b
        }else{
          a
        }
      }).map(x=>x._2).cache()

    val reducedRDD = containRels.map(x=>Row(x._1,x._2,x._3))

    //convert back to a df
    val schemaString = "subject rel object"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val allWithinDf = spark.createDataFrame(reducedRDD, schema)

    //now perform union with the EC relations to create final RCC-8 network
    ec = ec.select($"subject",$"rel",$"object")
    val initRCC8Network = allWithinDf.union(ec)

    println("RCC8 Network:"+initRCC8Network.count)
    initRCC8Network.cache()
    initRCC8Network.show(false)

    //save as a dataframe
    initRCC8Network.write.parquet(rcc8OutputDir+"/df")

    //save as text
    val finalRelsRDD = initRCC8Network
      .rdd
      .map(row=>row(0)+" "+row(1)+" "+row(2)+" 1")
    finalRelsRDD.take(20).foreach(x=>println(x))
    finalRelsRDD.repartition(partitions).saveAsTextFile(rcc8OutputDir+"/txt")




  }

}

