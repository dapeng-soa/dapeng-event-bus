organization := "com.today"

name := "event-bus"

version := "0.1-SNAPSHOT"

scalaVersion := "2.12.4"


resolvers ++= List("today nexus" at "http://nexus-inner.today36524.com.cn/repository/maven-public/")

publishTo := Some("today-snapshots" at "http://nexus-inner.today36524.com.cn/repository/maven-snapshots/")

credentials += Credentials("Sonatype Nexus Repository Manager", "nexus-inner.today36524.com.cn", "central-services", "E@Z.nrW3")

libraryDependencies ++= Seq(
  "com.github.wangzaixiang" %% "scala-sql" % "2.0.3",
  "com.github.dapeng" % "dapeng-core" % "2.0.1-SNAPSHOT",
  "com.github.dapeng" % "dapeng-message-kafka" % "2.0.1-SNAPSHOT",
  "org.slf4j" % "slf4j-api" % "1.7.13",
  "org.springframework" % "spring-jdbc" % "4.3.5.RELEASE",
  "org.springframework" % "spring-aop" % "4.3.5.RELEASE",
  "org.springframework" % "spring-aspects" % "4.3.5.RELEASE"
)

javacOptions ++= Seq("-encoding", "UTF-8")
