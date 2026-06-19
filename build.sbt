name := "etl-pipeline"
version := "1.0.0"
scalaVersion := "2.13.12"

val sparkVersion = "4.0.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  "org.yaml" % "snakeyaml" % "2.0",

  "mysql" % "mysql-connector-java" % "8.0.33",
  "org.postgresql" % "postgresql" % "42.7.7",

  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.apache.spark" %% "spark-core" % sparkVersion % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test
)