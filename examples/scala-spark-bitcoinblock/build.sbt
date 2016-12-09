lazy val root = (project in file(".")).
  settings(
    name := "example-hcl-spark-scala-bitcoinblock",
    version := "0.1"
  )

crossScalaVersions := Seq("2.10.5", "2.11.7")

resolvers += Resolver.mavenLocal

scalacOptions += "-target:jvm-1.7"

assemblyJarName in assembly := "example-hcl-spark-scala-bitcoinblock.jar"

libraryDependencies += "com.github.zuinnote" % "hadoopcryptoledger-fileformat" % "1.0.3" % "compile"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.5.0" % "provided"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test"

