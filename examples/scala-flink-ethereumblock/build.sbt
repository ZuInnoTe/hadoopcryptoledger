
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



crossScalaVersions := Seq("2.11.12")

resolvers += Resolver.mavenLocal

scalacOptions += "-target:jvm-1.8"

assemblyJarName in assembly := "example-hcl-flink-scala-ethereumblock.jar"

fork  := true




libraryDependencies += "com.github.zuinnote" % "hadoopcryptoledger-flinkdatasource" % "1.3.0" % "compile"
libraryDependencies += "com.github.zuinnote" % "hadoopcryptoledger-fileformat" % "1.3.0" % "compile"
// needed for certain functionality related to Ethereum in EthereumUtil
libraryDependencies += "org.bouncycastle" % "bcprov-ext-jdk15on" % "1.68" % "compile"
libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.12.1" % "provided"



libraryDependencies += "org.apache.flink" %% "flink-clients" % "1.12.1" % "it"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.1.0" % "it"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.5" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.5" % "it"  classifier "" classifier "tests"
