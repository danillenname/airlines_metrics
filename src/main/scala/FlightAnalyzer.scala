package com.example

import jobs.AirlinesMetricsJob
import metrics.Metrics
import readers.CsvReader
import writers.CsvWriter

object FlightAnalyzer extends Context {

  override val appName: String = "FlightsMetrics"
  override val master: String = Config.get("master")

  def main(args: Array[String]): Unit = {

    //пути к файлам с данными передаются через параметры.
    //также указываются папка, куда будут записаны результаты расчетов - metricsPath и путь до файла с информацией по прогруженным датам - metaPath
    val airlinesPath = args(0)
    val airportsPath = args(1)
    val flightsPath = args(2)
    val metaPath = args(3)
    val metricsPath = args(4)

    val filesPath = Map(
      "airlinesPath" -> airlinesPath,
      "airportsPath" -> airportsPath,
      "flightsPath" -> flightsPath,
      "metaPath" -> metaPath,
      "metricsPath" -> metricsPath)

    spark.sparkContext.setLogLevel("ERROR")

    //запуск расчета с передачей конфигов
    val job = new AirlinesMetricsJob(
      spark,
      AirlinesMetricsJob.Config(
        CsvReader.Config(),
        Metrics.Config("desc", 10),
        CsvWriter.Config()
      ),
      filesPath
    )

    job.run()

  }
}
