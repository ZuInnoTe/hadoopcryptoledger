lazy val root = (project in file(".")).
  settings(
    name := "example-hcl-spark-scala-ds-bitcoinblock",
    version := "0.1",
    scalaVersion := "2.10.4"
  )

scalacOptions += "-target:jvm-1.7"

resolvers += Resolver.mavenLocal

assemblyJarName in assembly := "example-hcl-spark-scala-ds-bitcoinblock.jar"

libraryDependencies += "com.github.zuinnote" % "spark-hadoopcryptoledger-ds_2.10" % "1.0.2" % "compile"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.0" % "provided"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.5.0" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.0" % "provided"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test"

