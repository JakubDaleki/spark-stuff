name := "spark-utils"

version := "0.1"

scalaVersion := "2.13.6"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.2.0"
libraryDependencies += "com.microsoft.sqlserver" % "mssql-jdbc" % "9.4.0.jre8"