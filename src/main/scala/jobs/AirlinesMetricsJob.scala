package com.example
package jobs

import metrics.Metrics
import readers.{CsvReader, ParquetReader}
import schemas.Schemas
import transformers.TransformersFunc
import writers.{CsvWriter, ParquetWriter}

import org.apache.log4j.LogManager
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.util.Try

object AirlinesMetricsJob {
  case class Config(readerConf: CsvReader.Config, metricsConf: Metrics.Config, writerConf: CsvWriter.Config)

}

class AirlinesMetricsJob(spark: SparkSession, config: AirlinesMetricsJob.Config, filesPath: Map[String, String]) {

  def run(): Unit = {

    try {
      val writerCsv = newCsvWriter()
      val writerParquet = newParquetWriter()

      val readerCsv = newCsvReader()
      val readerParquet = newParquetReader()

      val airlinesDf = readerCsv.read(filesPath("airlinesPath"), Schemas.airlinesSchema)
      val airportsDf = readerCsv.read(filesPath("airportsPath"), Schemas.airportsSchema)
      val flightsFullDf = readerCsv.read(filesPath("flightsPath"), Schemas.flightsSchema)
      val metaDf = readerCsv.read(filesPath("metaPath"), Schemas.metaSchema)

      //получение максимальной ранее прогруженной даты
      val maxCollectedDt = TransformersFunc.getMaxCollectedDt(metaDf)

      //отбор новых данных
      val flightsDf = flightsFullDf
        .transform(TransformersFunc.getNewRows(maxCollectedDt))

      //прекращение работы в случае отсутствия обновлений
      if (flightsDf.isEmpty) return

      //получение предыдущих состояний по каждой из метрик для дальнейшей агрегации
      val popularAirportsPrev = Try(readerParquet.read(filesPath("metricsPath") + "/popularAirports", Schemas.popularAirportsSchema))
        .getOrElse(spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schemas.popularAirportsSchema))
      val punctualAirlinesPrev = Try(readerParquet.read(filesPath("metricsPath") + "/punctualAirlines", Schemas.punctualAirlinesSchema))
        .getOrElse(spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schemas.punctualAirlinesSchema))
      val punctualAirlinesByAirportPrev = Try(readerParquet.read(filesPath("metricsPath") + "/punctualAirlinesByAirport", Schemas.punctualAirlinesByAirportSchema))
        .getOrElse(spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schemas.punctualAirlinesByAirportSchema))
      val punctualDestAirportByAirportPrev = Try(readerParquet.read(filesPath("metricsPath") + "/punctualDestAirportByAirport", Schemas.punctualDestAirportByAirportSchema))
        .getOrElse(spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schemas.punctualDestAirportByAirportSchema))
      val punctualDaysPrev = Try(readerParquet.read(filesPath("metricsPath") + "/punctualDays", Schemas.punctualDaysSchema))
        .getOrElse(spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schemas.punctualDaysSchema))
      val delayReasonsCntPrev = Try(readerParquet.read(filesPath("metricsPath") + "/delayReasonsCnt", Schemas.delayReasonsCntSchema))
        .getOrElse(spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schemas.delayReasonsCntSchema))
      val delayReasonsPercentagePrev = Try(readerParquet.read(filesPath("metricsPath") + "/delayReasonsPercentage", Schemas.delayReasonsPercentageSchema))
        .getOrElse(spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schemas.delayReasonsPercentageSchema))

      val metricsFunc = new Metrics()

      //расчет каждой из метрик
      val popularAirports = flightsDf
        .transform(metricsFunc.getPopularAirports(airportsDf, popularAirportsPrev))

      val punctualAirlines = flightsDf
        .transform(metricsFunc.getPunctualAirlines(airlinesDf, punctualAirlinesPrev))

      val punctualAirlinesByAirport = flightsDf
        .transform(metricsFunc.getPunctualByAirports(airlinesDf, airportsDf, "airline", punctualAirlinesByAirportPrev))
      val punctualDestAirportByAirport = flightsDf
        .transform(metricsFunc.getPunctualByAirports(airlinesDf, airportsDf, "destination_airport", punctualDestAirportByAirportPrev))

      val punctualDays = flightsDf
        .transform(metricsFunc.getPunctualDays(punctualDaysPrev))

      val delayReasonsCnt = flightsDf
        .transform(metricsFunc.getDelayReasonCnt(delayReasonsCntPrev))

      val delayReasonsPercentage = flightsDf
        .transform(metricsFunc.getDelayReasonPercentage(delayReasonsPercentagePrev))

      //вывод рассчитанных метрик
      println("Топ-10 самых популярных аэропортов по количеству совершаемых полетов:")
      showResult(popularAirports, "flights_amt")

      println("Топ-10 авиакомпаний, вовремя выполняющих рейсы:")
      showResult(punctualAirlines, "on_time_flights_amt")

      println("Топ-10 перевозчиков для каждого аэропорта на основании вовремя совершенного вылета:")
      showResultForPunctualByAirport(punctualAirlinesByAirport, "airline", "on_time_flights_amt")

      println("Топ-10 аэропортов назначения для каждого аэропорта на основании вовремя совершенного вылета:")
      showResultForPunctualByAirport(punctualDestAirportByAirport, "destination_airport", "on_time_flights_amt")

      println("Дни недели в порядке своевременности прибытия рейсов, совершаемых в эти дни:")
      showResult(punctualDays, "on_time_flights_amt")

      println("Количество задержанных рейсов по каждой из причин:")
      delayReasonsCnt.show()

      println("Доля минут задержки каждой из причин от общего количества минут задержки рейсов:")
      delayReasonsPercentage
        .select(
          "air_system_delay_time_percentage",
          "security_delay_time_percentage",
          "airline_delay_time_percentage",
          "late_aircraft_delay_time_percentage",
          "weather_delay_time_percentage",
          "processed_dttm"
        )
        .show()

      //запись рассчитанных метрик
      writerParquet.writeParquetTable(
        filesPath("metricsPath") + "/popularAirports",
        SaveMode.Overwrite,
        Schemas.delayReasonsPercentageSchema)(popularAirports)
      writerParquet.writeParquetTable(
        filesPath("metricsPath") + "/punctualAirlines",
        SaveMode.Overwrite,
        Schemas.delayReasonsPercentageSchema)(punctualAirlines)
      writerParquet.writeParquetTable(
        filesPath("metricsPath") + "/punctualAirlinesByAirport",
        SaveMode.Overwrite,
        Schemas.delayReasonsPercentageSchema)(punctualAirlinesByAirport)
      writerParquet.writeParquetTable(
        filesPath("metricsPath") + "/punctualDestAirportByAirport",
        SaveMode.Overwrite,
        Schemas.delayReasonsPercentageSchema)(punctualDestAirportByAirport)
      writerParquet.writeParquetTable(
        filesPath("metricsPath") + "/punctualDays",
        SaveMode.Overwrite,
        Schemas.delayReasonsPercentageSchema)(punctualDays)
      writerParquet.writeParquetTable(
        filesPath("metricsPath") + "/delayReasonsCnt",
        SaveMode.Overwrite,
        Schemas.delayReasonsPercentageSchema)(delayReasonsCnt)
      writerParquet.writeParquetTable(
        filesPath("metricsPath") + "/delayReasonsPercentage",
        SaveMode.Overwrite,
        Schemas.delayReasonsPercentageSchema)(delayReasonsPercentage)

      //получение и запись данных о промежутке рассчитанных дат
      val collectedDtDf = flightsDf
        .transform(TransformersFunc.getPeriodOfDates)

      val (minDate, maxDate, processedDttm) = (
        collectedDtDf.collect()(0)(0),
        collectedDtDf.collect()(0)(1),
        collectedDtDf.collect()(0)(2))

      writerCsv.writeMeta(minDate.toString, maxDate.toString, processedDttm.toString, filesPath("metaPath"))
    }
    catch {
      case ex: Throwable => {
        val log = LogManager.getRootLogger
        log.error(ex.toString)
      }
    }
  }

  protected def newCsvReader(): DataFrameReader = {
    new CsvReader(spark, config.readerConf)
  }

  protected def newCsvWriter(): CsvWriter = {
    new CsvWriter(config.writerConf)
  }

  protected def newParquetReader(): DataFrameReader = {
    new ParquetReader(spark)
  }

  protected def newParquetWriter(): ParquetWriter = {
    new ParquetWriter(spark)
  }

  protected def showResult(df: DataFrame, orderCol: String, metricsConf: Metrics.Config = config.metricsConf): Unit = {
    val ordering = TransformersFunc.getOrdering(metricsConf.order, orderCol)

    df
      .orderBy(ordering)
      .limit(metricsConf.recAmt)
      .show(false)

  }

  protected def showResultForPunctualByAirport(df: DataFrame, partitionCol: String, orderCol: String, metricsConf: Metrics.Config = config.metricsConf): Unit = {
    val ordering = TransformersFunc.getOrdering(metricsConf.order, orderCol)

    df
      .transform(TransformersFunc.withRowNum(Array("airport", partitionCol), TransformersFunc.getOrdering(config.metricsConf.order, orderCol)))
      .filter(col("row_num") <= config.metricsConf.recAmt)
      .drop("row_num")
      .orderBy(col("airport"), ordering)
      .show(false)


  }
}


