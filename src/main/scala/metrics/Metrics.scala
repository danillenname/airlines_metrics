package com.example
package metrics

import transformers._

import org.apache.spark.sql.functions.{col, round, sum, when}
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

object Metrics {
  case class Config(order: String, recAmt: Int)
}

class Metrics() {

  private val flightsAirportsJoinCond: Column = col("l.origin_airport") === col("r.iata_code")
  private val flightsAirlinesJoinCond: Column = col("l.airline") === col("r.iata_code")

  private def getCount(groupByCols: Array[String])(df: DataFrame): DataFrame =
    df
      .groupBy(groupByCols.head, groupByCols.tail: _*)
      .count()

  def getPopularAirports(airportsDf: DataFrame, prevState: DataFrame)(flightsDf: DataFrame): Dataset[Row] = {
    flightsDf
      .transform(getCount(Array("origin_airport")))
      .transform(TransformersFunc.joinDf(
        airportsDf,
        flightsAirportsJoinCond,
        "inner",
        Array(col("r.airport"), col("l.count").as("flights_amt")),
        doBroadcast = true
      ))
      .union(prevState.select(
        col("airport"),
        col("flights_amt")
      ))
      .groupBy("airport")
      .agg(
        sum(col("flights_amt")).as("flights_amt")
      )
      .transform(TransformersFunc.withProcessedDttm)
  }

  def getPunctualAirlines(airlinesDf: DataFrame, prevState: DataFrame)(flightsDf: DataFrame): Dataset[Row] = {
    flightsDf
      .transform(TransformersFunc.isOnTimeFlights)
      .transform(getCount(Array("airline")))
      .transform(TransformersFunc.joinDf(
        airlinesDf,
        flightsAirlinesJoinCond,
        "inner",
        Array(col("r.airline"), col("l.count").as("on_time_flights_amt")),
        doBroadcast = true
      ))
      .union(prevState.select(
        col("airline"),
        col("on_time_flights_amt")
      ))
      .groupBy("airline")
      .agg(
        sum(col("on_time_flights_amt")).as("on_time_flights_amt")
      )
      .transform(TransformersFunc.withProcessedDttm)

  }

  def getPunctualByAirports(airlinesDf: DataFrame, airportsDf: DataFrame, aggCol: String, prevState: DataFrame)(flightsDf: DataFrame): Dataset[Row] = {

    val secondJoinDf = aggCol match {
      case "airline" => airlinesDf
      case "destination_airport" => airportsDf
    }

    val secondJoinCond = aggCol match {
      case "airline" => flightsAirlinesJoinCond
      case "destination_airport" => col("l.destination_airport") === col("r.iata_code")
    }

    val selectedCol = aggCol match {
      case "airline" => col("r.airline")
      case "destination_airport" => col("r.airport").as("destination_airport")
    }

    flightsDf
      .transform(TransformersFunc.isOnTimeFlights)
      .transform(getCount(Array("origin_airport", aggCol)))
      .transform(TransformersFunc.joinDf(
        airportsDf,
        flightsAirportsJoinCond,
        "inner",
        Array(col(s"l.$aggCol"), col("l.count"), col("r.airport")),
        doBroadcast = true
      ))
      .transform(TransformersFunc.joinDf(
        secondJoinDf,
        secondJoinCond,
        "inner",
        Array(col("l.airport"), selectedCol, col("l.count").as("on_time_flights_amt")),
        doBroadcast = true))
      .union(prevState.select(
        col("airport"),
        col(aggCol),
        col("on_time_flights_amt")
      ))
      .groupBy("airport", aggCol)
      .agg(
        sum(col("on_time_flights_amt")).as("on_time_flights_amt")
      )
      .transform(TransformersFunc.withProcessedDttm)

  }

  def getPunctualDays(prevState: DataFrame)(flightsDf: DataFrame): Dataset[Row] = {
    flightsDf
      .transform(TransformersFunc.isOnTimeFlights)
      .transform(getCount(Array("day_of_week")))
      .select(
        col("day_of_week"),
        col("count").as("on_time_flights_amt"))
      .union(prevState.select(
        col("day_of_week"),
        col("on_time_flights_amt")
      ))
      .groupBy("day_of_week")
      .agg(
        sum(col("on_time_flights_amt")).as("on_time_flights_amt")
      )
      .transform(TransformersFunc.withProcessedDttm)

  }

  def getDelayReasonCnt(prevState: DataFrame)(flightsDf: DataFrame): DataFrame = {
    def delayedFlightsCnt(reason: String): Column = sum(when(col(reason) > 0, 1).otherwise(0)).as(s"${reason}_cnt")

    flightsDf
      .transform(TransformersFunc.isDelayed)
      .select(
        delayedFlightsCnt("air_system_delay"),
        delayedFlightsCnt("security_delay"),
        delayedFlightsCnt("airline_delay"),
        delayedFlightsCnt("late_aircraft_delay"),
        delayedFlightsCnt("weather_delay")
      )
      .union(prevState.select(
        col("air_system_delay_cnt"),
        col("security_delay_cnt"),
        col("airline_delay_cnt"),
        col("late_aircraft_delay_cnt"),
        col("weather_delay_cnt")
      ))
      .transform(TransformersFunc.withProcessedDttm)
      .groupBy("processed_dttm")
      .agg(
        sum(col("air_system_delay_cnt")).as("air_system_delay_cnt"),
        sum(col("security_delay_cnt")).as("security_delay_cnt"),
        sum(col("airline_delay_cnt")).as("airline_delay_cnt"),
        sum(col("late_aircraft_delay_cnt")).as("late_aircraft_delay_cnt"),
        sum(col("weather_delay_cnt")).as("weather_delay_cnt")
      )
      .select(
        col("air_system_delay_cnt"),
        col("security_delay_cnt"),
        col("airline_delay_cnt"),
        col("late_aircraft_delay_cnt"),
        col("weather_delay_cnt"),
        col("processed_dttm")
      )

  }

  def getDelayReasonPercentage(prevState: DataFrame)(flightsDf: DataFrame): DataFrame = {
    def delayTime(reason: String): Column = sum(reason)

    val delayTimeTotal = col("air_system_delay_time") +
      col("security_delay_time") +
      col("airline_delay_time") +
      col("late_aircraft_delay_time") +
      col("weather_delay_time")

    def delayTimePercentage(reason: String): Column = {
      round(col(reason) / delayTimeTotal, 6).as(s"${reason}_percentage")
    }

    flightsDf
      .transform(TransformersFunc.isDelayed)
      .select(
        delayTime("air_system_delay").as("air_system_delay_time"),
        delayTime("security_delay").as("security_delay_time"),
        delayTime("airline_delay").as("airline_delay_time"),
        delayTime("late_aircraft_delay").as("late_aircraft_delay_time"),
        delayTime("weather_delay").as("weather_delay_time")
      )
      .union(prevState.select(
        col("air_system_delay_time"),
        col("security_delay_time"),
        col("airline_delay_time"),
        col("late_aircraft_delay_time"),
        col("weather_delay_time")
      ))
      .transform(TransformersFunc.withProcessedDttm)
      .groupBy("processed_dttm")
      .agg(
        sum(col("air_system_delay_time")).as("air_system_delay_time"),
        sum(col("security_delay_time")).as("security_delay_time"),
        sum(col("airline_delay_time")).as("airline_delay_time"),
        sum(col("late_aircraft_delay_time")).as("late_aircraft_delay_time"),
        sum(col("weather_delay_time")).as("weather_delay_time")
      )
      .select(
        col("air_system_delay_time"),
        col("security_delay_time"),
        col("airline_delay_time"),
        col("late_aircraft_delay_time"),
        col("weather_delay_time"),
        delayTimePercentage("air_system_delay_time"),
        delayTimePercentage("security_delay_time"),
        delayTimePercentage("airline_delay_time"),
        delayTimePercentage("late_aircraft_delay_time"),
        delayTimePercentage("weather_delay_time"),
        col("processed_dttm")
      )

  }
}
