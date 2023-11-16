package com.example

import org.apache.spark.sql.SparkSession

trait Context {
  val appName: String
  val master: String

  private def createSessionWith(appName: String, master: String) = {
    SparkSession.builder()
      .appName(appName)
      .master(master)
      .getOrCreate()
  }

  lazy val spark: SparkSession = createSessionWith(appName, master)

}
