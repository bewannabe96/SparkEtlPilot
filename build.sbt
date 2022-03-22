name         := "SparkETLPilot"
version      := "0.1.0"
scalaVersion := "2.12.14"

libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.1" % Compile

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.3" % Provided
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.3" % Provided