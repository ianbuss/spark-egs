name := "Format Tests"

version := "1.0"

scalaVersion := "2.10.3"

scalacOptions += "-deprecation"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.0.0-cdh5.1.3",
  "org.apache.avro" % "avro" % "1.7.5",
  "com.twitter" % "parquet-avro" % "1.2.5"
)

resolvers ++= Seq(
  "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository" at "http://repo.akka.io/releases/"
)
