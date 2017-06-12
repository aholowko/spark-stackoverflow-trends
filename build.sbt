name := "spark-stackoverflow-trends"

version := "1.1.2"
scalaVersion := "2.11.8"
val sparkVersion = "2.0.2"

logBuffered in Test := false

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % "compile" exclude("org.scalatest", "scalatest_2.11"),
    "org.apache.spark" %% "spark-sql" % sparkVersion % "compile" exclude("org.scalatest", "scalatest_2.11"),
    "com.github.nscala-time" %% "nscala-time" % "2.16.0",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

// Do not include Scala in the assembled JAR
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

//test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "overview.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}