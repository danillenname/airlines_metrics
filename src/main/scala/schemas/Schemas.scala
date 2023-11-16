package com.example
package schemas

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object Schemas {

  val airlinesSchema: StructType = StructType(Seq(
    StructField("iata_code", StringType),
    StructField("airline", StringType))
  )

  val airportsSchema: StructType = StructType(Seq(
    StructField("iata_code", StringType),
    StructField("airport", StringType),
    StructField("city", StringType),
    StructField("state", StringType),
    StructField("country", StringType),
    StructField("latitude", StringType),
    StructField("longitude", StringType))
  )

  val flightsSchema: StructType = StructType(Seq(
    StructField("year", IntegerType),
    StructField("month", IntegerType),
    StructField("day", IntegerType),
    StructField("day_of_week", IntegerType),
    StructField("airline", StringType),
    StructField("flight_number", IntegerType),
    StructField("tail_number", StringType),
    StructField("origin_airport", StringType),
    StructField("destination_airport", StringType),
    StructField("scheduled_departure", StringType),
    StructField("departure_time", StringType),
    StructField("departure_delay", IntegerType),
    StructField("taxi_out", IntegerType),
    StructField("wheels_off", StringType),
    StructField("scheduled_time", IntegerType),
    StructField("elapsed_time", IntegerType),
    StructField("air_time", IntegerType),
    StructField("distance", IntegerType),
    StructField("wheels_on", StringType),
    StructField("taxi_in", IntegerType),
    StructField("scheduled_arrival", StringType),
    StructField("arrival_time", StringType),
    StructField("arrival_delay", IntegerType),
    StructField("diverted", IntegerType),
    StructField("cancelled", IntegerType),
    StructField("cancellation_reason", StringType),
    StructField("air_system_delay", IntegerType),
    StructField("security_delay", IntegerType),
    StructField("airline_delay", IntegerType),
    StructField("late_aircraft_delay", IntegerType),
    StructField("weather_delay", IntegerType))
  )

  val metaSchema: StructType = StructType(Seq(
    StructField("min_date_collected", StringType),
    StructField("max_date_collected", StringType),
    StructField("processed_dttm", StringType))
  )

  val popularAirportsSchema: StructType = StructType(Seq(
    StructField("airport", StringType),
    StructField("flights_amt", IntegerType),
    StructField("processed_dttm", StringType))
  )

  val punctualAirlinesSchema: StructType = StructType(Seq(
    StructField("airline", StringType),
    StructField("on_time_flights_amt", IntegerType),
    StructField("processed_dttm", StringType))
  )

  val punctualAirlinesByAirportSchema: StructType = StructType(Seq(
    StructField("airport", StringType),
    StructField("airline", StringType),
    StructField("on_time_flights_amt", IntegerType),
    StructField("processed_dttm", StringType))
  )

  val punctualDestAirportByAirportSchema: StructType = StructType(Seq(
    StructField("airport", StringType),
    StructField("destination_airport", StringType),
    StructField("on_time_flights_amt", IntegerType),
    StructField("processed_dttm", StringType))
  )

  val punctualDaysSchema: StructType = StructType(Seq(
    StructField("day_of_week", StringType),
    StructField("on_time_flights_amt", IntegerType),
    StructField("processed_dttm", StringType))
  )

  val delayReasonsCntSchema: StructType = StructType(Seq(
    StructField("air_system_delay_cnt", IntegerType),
    StructField("security_delay_cnt", IntegerType),
    StructField("airline_delay_cnt", IntegerType),
    StructField("late_aircraft_delay_cnt", IntegerType),
    StructField("weather_delay_cnt", IntegerType),
    StructField("processed_dttm", StringType))
  )

  val delayReasonsPercentageSchema: StructType = StructType(Seq(
    StructField("air_system_delay_time", IntegerType),
    StructField("security_delay_time", IntegerType),
    StructField("airline_delay_time", IntegerType),
    StructField("late_aircraft_delay_time", IntegerType),
    StructField("weather_delay_time", IntegerType),
    StructField("air_system_delay_time_percentage", DoubleType),
    StructField("security_delay_time_percentage", DoubleType),
    StructField("airline_delay_time_percentage", DoubleType),
    StructField("late_aircraft_delay_time_percentage", DoubleType),
    StructField("weather_delay_time_percentage", DoubleType),
    StructField("processed_dttm", StringType))
  )

}
