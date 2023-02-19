scalaVersion := "2.12.15"

version := "0.1"

name := "StreamHandler"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.3.0",
    "org.apache.spark" %% "spark-sql" % "3.3.0"
)
