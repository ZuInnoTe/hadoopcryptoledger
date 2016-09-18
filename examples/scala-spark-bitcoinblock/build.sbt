lazy val root = (project in file(".")).
  settings(
    name := "example-hcl-spark-scala-bitcoinblock",
    version := "0.1",
    scalaVersion := "2.10.4"
  )

scalacOptions += "-target:jvm-1.7"

assemblyJarName in assembly := "example-hcl-spark-scala-bitcoinblock.jar"

libraryDependencies += "com.github.zuinnote" % "hadoopcryptoledger-fileformat" % "1.0.0" % "compile"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.0" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.0" % "provided"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.1" % "test"

