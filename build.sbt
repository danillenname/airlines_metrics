name := "spark-course-air"

version := "0.1"

scalaVersion := "2.12.15"

lazy val sparkVersion = "3.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.github.tototoshi" %% "scala-csv" % "1.3.10"
)

idePackagePrefix := Option("com.example")