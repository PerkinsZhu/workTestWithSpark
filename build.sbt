name := "workTestWithSpark"

version := "0.1"

scalaVersion := "2.11.12"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"


// https://mvnrepository.com/artifact/junit/junit
libraryDependencies += "junit" % "junit" % "4.12"

//mongodb 官方依赖
libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.3.0"
)

//为基准测试插件
resolvers += "Sonatype OSS Snapshots" at
  "https://oss.sonatype.org/content/repositories/releases"

libraryDependencies += "com.storm-enroute" %% "scalameter" % "0.7"
fork in Test := true

// https://mvnrepository.com/artifact/log4j/log4j
//libraryDependencies += "log4j" % "log4j" % "1.2.17"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.6.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.6.0"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.6"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.6"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.9.6"

libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.6"


libraryDependencies += "org.apache.hbase" % "hbase" % "1.4.5"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.4.5"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.4.5"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.4.5"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.0.0"

libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.12"

libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.4.13"

libraryDependencies += "com.alibaba" % "dubbo" % "2.6.2"

libraryDependencies += "org.apache.hive" % "hive-exec" % "1.1.0"
libraryDependencies += "org.apache.hive" % "hive-metastore" % "1.1.0"
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.1.0"


libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.11.1"

libraryDependencies += "io.dgraph" % "dgraph4j" % "1.7.1"

/*assemblyMergeStrategy in run := {
  case PathList("com.fasterxml", "jackson.databind", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in run).value
    oldStrategy(x)
}*/

libraryDependencies += "com.google.guava" % "guava" % "27.0.1-jre"

libraryDependencies += "io.grpc" % "grpc-stub" % "1.18.0"
libraryDependencies += "io.grpc" % "grpc-core" % "1.18.0"
libraryDependencies += "io.grpc" % "grpc-netty" % "1.18.0"
libraryDependencies += "io.grpc" % "grpc-protobuf" % "1.18.0"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.5"



