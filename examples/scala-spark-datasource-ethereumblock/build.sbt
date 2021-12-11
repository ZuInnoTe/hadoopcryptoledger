

import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
    name := "example-hcl-spark-scala-ds-ethereumblock",
    version := "0.1"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)



crossScalaVersions := Seq( "2.11.10")

scalacOptions += "-target:jvm-1.8"

resolvers += Resolver.mavenLocal


fork  := true



assemblyJarName in assembly := "example-hcl-spark-scala-ds-ethereumblock.jar"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

libraryDependencies += "com.github.zuinnote" %% "spark-hadoopcryptoledger-ds" % "1.3.1" % "compile"
// needed for enriched EthereumBlocks
libraryDependencies += "org.bouncycastle" % "bcprov-ext-jdk15on" % "1.70" % "compile"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.7" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.0" % "provided"


libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"


libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-minicluster" % "2.7.0" % "it"
