organization := "com.today"

name := "event-bus"

version := "0.1-SNAPSHOT"

scalaVersion := "2.12.4"


resolvers ++= List("today nexus" at "http://nexus-inner.today36524.com.cn/repository/maven-public/")

publishTo := Some("today-snapshots" at "http://nexus-inner.today36524.com.cn/repository/maven-snapshots/")

credentials += Credentials("Sonatype Nexus Repository Manager", "nexus-inner.today36524.com.cn", "central-services", "E@Z.nrW3")

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.13",
  "org.springframework" % "spring-aop" % "4.3.5.RELEASE",
  "org.springframework" % "spring-aspects" % "4.3.5.RELEASE",
  "org.apache.kafka" % "kafka-clients" % "1.0.0",
  "com.github.wangzaixiang" %% "scala-sql" % "2.0.3",
  "com.github.dapeng" % "dapeng-core" % "2.0.1-SNAPSHOT",
  "com.github.dapeng" % "dapeng-utils" % "2.0.1-SNAPSHOT",
  "com.alibaba.otter" % "canal.common" % "1.0.25",
  "com.alibaba.otter" % "canal.protocol" % "1.0.25",
  "com.github.wangzaixiang" %% "spray-json" % "1.3.4"
)

javacOptions ++= Seq("-encoding", "UTF-8")
