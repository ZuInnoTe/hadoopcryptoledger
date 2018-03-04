
 
import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
    name := "example-hcl-spark-scala-ds-bitcoinblock",
    version := "0.1"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)
.enablePlugins(JacocoItPlugin)
 


crossScalaVersions := Seq("2.10.5", "2.11.7")

scalacOptions += "-target:jvm-1.7"

resolvers += Resolver.mavenLocal


fork  := true


assemblyJarName in assembly := "example-hcl-spark-scala-ds-bitcoinblock.jar"

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}

libraryDependencies += "com.github.zuinnote" %% "spark-hadoopcryptoledger-ds" % "1.1.4" % "compile"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.1" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.1" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.0" % "provided"


libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"


libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-minicluster" % "2.7.0" % "it"


