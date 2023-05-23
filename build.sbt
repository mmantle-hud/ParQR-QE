
name := "ParQR-QE"

version := "1.0"

scalaVersion := "2.12.11"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.0"
//% "provided"
libraryDependencies += "org.apache.sedona" % "sedona-core-3.0_2.12" % "1.2.1-incubating"
libraryDependencies += "org.apache.sedona" % "sedona-sql-3.0_2.12" % "1.2.1-incubating"
libraryDependencies += "org.apache.sedona" % "sedona-viz-3.0_2.12" % "1.2.1-incubating"
libraryDependencies += "org.locationtech.jts"% "jts-core"% "1.18.0" % "compile"
libraryDependencies += "org.wololo" % "jts2geojson" % "0.14.3" % "compile"
libraryDependencies += "org.datasyslab" % "geotools-wrapper" % "1.1.0-25.2" % "compile"
libraryDependencies += "info.debatty" % "java-string-similarity" % "1.2.1"
libraryDependencies += "org.apache.jena" % "jena-arq" % "3.1.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}