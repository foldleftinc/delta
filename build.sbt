name := "au.com.aeonsoftware.delta.sample"

version := "0.1"

scalaVersion := "2.12.14"
enablePlugins(JavaAppPackaging)

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "3.1.2"
libraryDependencies += "io.delta" %% "delta-core" % "1.0.0"

