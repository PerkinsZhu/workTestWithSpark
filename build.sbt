name := "workTestWithSpark"

version := "0.1"

scalaVersion := "2.11.12"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.0.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.1" % "provided"



// https://mvnrepository.com/artifact/junit/junit
libraryDependencies += "junit" % "junit" % "4.12"

//mongodb 官方依赖
libraryDependencies ++= Seq(
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.1"
)

//为基准测试插件
resolvers += "Sonatype OSS Snapshots" at
  "https://oss.sonatype.org/content/repositories/releases"

libraryDependencies += "com.storm-enroute" %% "scalameter" % "0.7"
fork in Test := true

// https://mvnrepository.com/artifact/log4j/log4j
libraryDependencies += "log4j" % "log4j" % "1.2.17"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-common
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.1.1"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.1.1"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-hdfs
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "3.1.1"
// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-mapreduce-client-core
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.1.1"

// https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-core
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.6"
// https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-databind
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.6"
// https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-annotations
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.9.6"

// https://mvnrepository.com/artifact/com.fasterxml.jackson.module/jackson-module-scala
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.6"


// https://mvnrepository.com/artifact/org.apache.hbase/hbase
libraryDependencies += "org.apache.hbase" % "hbase" % "1.4.5"
// https://mvnrepository.com/artifact/org.apache.hbase/hbase-common
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.4.5"
// https://mvnrepository.com/artifact/org.apache.hbase/hbase-client
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.4.5"
// https://mvnrepository.com/artifact/org.apache.hbase/hbase-server
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.4.5"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.0.0"



// https://mvnrepository.com/artifact/mysql/mysql-connector-java
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.12"

// https://mvnrepository.com/artifact/org.apache.zookeeper/zookeeper
libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.4.13"

// https://mvnrepository.com/artifact/com.alibaba/dubbo
libraryDependencies += "com.alibaba" % "dubbo" % "2.6.2"

// https://mvnrepository.com/artifact/org.apache.hive/hive-exec
libraryDependencies += "org.apache.hive" % "hive-exec" % "3.1.0"
// https://mvnrepository.com/artifact/org.apache.hive/hive-metastore
libraryDependencies += "org.apache.hive" % "hive-metastore" % "3.1.0"
// https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc
libraryDependencies += "org.apache.hive" % "hive-jdbc" % "3.1.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.1"

// https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-api
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.11.1"

/*assemblyMergeStrategy in run := {
  case PathList("com.fasterxml", "jackson.databind", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in run).value
    oldStrategy(x)
}*/