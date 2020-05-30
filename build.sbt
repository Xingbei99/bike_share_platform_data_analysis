name := "bike_share_platform_data_analysis"

version := "0.1"

scalaVersion := "2.11.12"

scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.3.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1"

libraryDependencies += "org.rogach" %% "scallop" % "3.1.2"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}