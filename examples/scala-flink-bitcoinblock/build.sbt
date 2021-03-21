
import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
    name := "example-hcl-flink-scala-bitcoinblock",
    version := "0.1"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)



crossScalaVersions := Seq("2.11.12")

resolvers += Resolver.mavenLocal

scalacOptions += "-target:jvm-1.8"

assemblyJarName in assembly := "example-hcl-flink-scala-bitcoinblock.jar"

fork  := true


libraryDependencies += "com.github.zuinnote" % "hadoopcryptoledger-flinkdatasource" % "1.3.0" % "compile"
libraryDependencies += "com.github.zuinnote" % "hadoopcryptoledger-fileformat" % "1.3.0" % "compile"
libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.12.1" % "provided"


// needed for writable serializer

libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.12.1" % "it"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.0" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.5" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.5" % "it"  classifier "" classifier "tests"
