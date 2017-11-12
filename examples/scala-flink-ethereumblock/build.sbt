
import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
    name := "example-hcl-flink-scala-ethereumblock",
    version := "0.1"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)



crossScalaVersions := Seq("2.10.5", "2.11.7")

resolvers += Resolver.mavenLocal

scalacOptions += "-target:jvm-1.7"

assemblyJarName in assembly := "example-hcl-flink-scala-ethereumblock.jar"

fork  := true

jacoco.settings

itJacoco.settings


libraryDependencies += "com.github.zuinnote" % "hadoopcryptoledger-flinkdatasource" % "1.1.0" % "compile"
libraryDependencies += "com.github.zuinnote" % "hadoopcryptoledger-fileformat" % "1.1.0" % "compile"
// needed for certain functionality related to Ethereum in EthereumUtil
libraryDependencies += "org.bouncycastle" % "bcprov-ext-jdk15on" % "1.58" % "compile"
libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.2.0" % "provided" 

libraryDependencies += "org.apache.flink" % "flink-shaded-hadoop2" % "1.2.0" % "provided"  

// needed for writable serializer 
libraryDependencies += "org.apache.flink" %% "flink-hadoop-compatibility" % "1.2.0" % "compile" 

libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.2.0" % "it" 

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.2.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.2.0" % "it"  classifier "" classifier "tests"
