
import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
    name := "example-hcl-spark-scala-ethereumblock",
    version := "0.1"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)



crossScalaVersions := Seq( "2.11.10")

resolvers += Resolver.mavenLocal

scalacOptions += "-target:jvm-1.8"

assemblyJarName in assembly := "example-hcl-spark-scala-ethereumblock.jar"

fork  := true


libraryDependencies += "com.github.zuinnote" % "hadoopcryptoledger-fileformat" % "1.3.0" % "compile"

// needed for certain functionality related to Ethereum in EthereumUtil
libraryDependencies += "org.bouncycastle" % "bcprov-ext-jdk15on" % "1.68" % "compile"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.0" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"


libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-minicluster" % "2.7.0" % "it"
