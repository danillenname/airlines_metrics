package com.example
package transformers

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{broadcast, col, concat, current_timestamp, lit, max, min, row_number, to_date}

import scala.util.Try

object TransformersFunc {

  private val concatDate = to_date(concat(col("year"), lit("-"), col("month"), lit("-"), col("day")))

  def isOnTimeFlights(df: DataFrame): DataFrame = {
    val isOnTime = col("departure_delay") <= 0 and col("arrival_delay") <= 0 and col("diverted") === 0

    df.filter(isOnTime).toDF()
  }

  def isDelayed(df: DataFrame): DataFrame =
    df.filter(col("air_system_delay").isNotNull)

  def withRowNum(partitionCols: Array[String], ordering: Column)(df: DataFrame): Dataset[Row] =
    df.withColumn("row_num", row_number().over(Window
      .partitionBy(partitionCols.head, partitionCols.tail: _*)
      .orderBy(ordering)))

  def withProcessedDttm(df: DataFrame): DataFrame =
    df.withColumn("processed_dttm", current_timestamp())

  def joinDf(
              joinedDf: DataFrame,
              joinCond: Column,
              joinType: String,
              selectedCols: Array[Column],
              doBroadcast: Boolean)
            (df: DataFrame): DataFrame = {
    val rightDf = if (doBroadcast) broadcast(joinedDf) else joinedDf

    df.as("l")
      .join(rightDf.as("r"), joinCond, joinType)
      .select(selectedCols: _*)
  }

  def getPeriodOfDates(df: DataFrame): DataFrame =
    df.select(
      min(concatDate).as("min_date_collected"),
      max(concatDate).as("max_date_collected"),
      current_timestamp().as("processed_dttm"))

  def getNewRows(prevMaxDt: String)(df: DataFrame): Dataset[Row] =
    df.filter(concatDate > prevMaxDt)

  def getMaxCollectedDt(df: DataFrame): String =
    Try(df.select(max(col("max_date_collected"))).collect()(0)(0).toString).getOrElse("1900-01-01")

  def getOrdering(order: String, orderCol: String): Column = {
    order match {
      case "desc" => col(orderCol).desc
      case "asc" => col(orderCol).asc
    }
  }
}
