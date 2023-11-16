package com.example
package readers

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object CsvReader {
  case class Config(separator: Char = ',', hasHeader: Boolean = true)

}

class CsvReader(spark: SparkSession, config: CsvReader.Config) extends DataFrameReader {
  override def read(path: String, schema: StructType): DataFrame =
    spark.read
      .option("header", config.hasHeader)
      .option("sep", config.separator.toString)
      .schema(schema)
      .csv(path)
}
