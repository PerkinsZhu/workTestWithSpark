name := "workTestWithSpark"

version := "0.1"

scalaVersion := "2.11.11"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
// https://mvnrepository.com/artifact/junit/junit
libraryDependencies += "junit" % "junit" % "4.12"
libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.2",
  "org.apache.spark" %% "spark-sql" % "2.2.0"
)

resolvers += "Sonatype OSS Snapshots" at
  "https://oss.sonatype.org/content/repositories/releases"

libraryDependencies += "com.storm-enroute" %% "scalameter" % "0.7"
fork in Test := true