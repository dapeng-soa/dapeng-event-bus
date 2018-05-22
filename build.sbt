organization := "com.today"

name := "event-bus"

version := "0.2-SNAPSHOT"

scalaVersion := "2.12.4"

resolvers ++= List("today nexus" at "http://nexus.today36524.td/repository/maven-public/")

publishTo := Some("today-snapshots" at "http://nexus.today36524.td/repository/maven-snapshots/")

credentials += Credentials("Sonatype Nexus Repository Manager", "nexus.today36524.td", "central-services", "E@Z.nrW3")

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.13",
  "org.springframework" % "spring-aop" % "4.3.5.RELEASE",
  "org.springframework" % "spring-aspects" % "4.3.5.RELEASE",
  "org.apache.kafka" % "kafka-clients" % "1.1.0",
  "com.github.wangzaixiang" %% "scala-sql" % "2.0.6",
  "com.github.wangzaixiang" %% "spray-json" % "1.3.4",
  "com.google.guava" % "guava" % "16.0.1",
  "com.alibaba.otter" % "canal.protocol" % "1.0.25" excludeAll(
    ExclusionRule().withOrganization("com.alibaba.otter").withName("canal.common"),
    ExclusionRule().withOrganization("commons-lang").withName("commons-lang")
  ),
  "com.github.dapeng" % "dapeng-open-api" % "2.0.2-SNAPSHOT",
  "org.apache.httpcomponents" % "httpclient" % "4.5.5",
  "org.simpleframework" % "simple-xml" % "2.7.1",
  "org.springframework.retry" % "spring-retry" % "1.2.2.RELEASE"
)

javacOptions ++= Seq("-encoding", "UTF-8")

//addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.9.0")

