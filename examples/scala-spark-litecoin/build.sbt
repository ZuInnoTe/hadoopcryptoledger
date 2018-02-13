 
import sbt._
import Keys._
import scala._


lazy val root = (project in file("."))
.settings(
    name := "example-hcl-spark-scala-litecoinlock",
    version := "0.1"
)
 .configs( IntegrationTest )
  .settings( Defaults.itSettings : _*)

 

crossScalaVersions := Seq("2.10.5", "2.11.7")

resolvers += Resolver.mavenLocal

scalacOptions += "-target:jvm-1.7"

assemblyJarName in assembly := "example-hcl-spark-scala-litecoinblock.jar"

fork  := true

jacoco.settings

itJacoco.settings


libraryDependencies += "com.github.zuinnote" % "hadoopcryptoledger-fileformat" % "1.1.3" % "compile"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test,it"

libraryDependencies += "javax.servlet" % "javax.servlet-api" % "3.0.1" % "it"


libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.0" % "it" classifier "" classifier "tests"

libraryDependencies += "org.apache.hadoop" % "hadoop-minicluster" % "2.7.0" % "it"


