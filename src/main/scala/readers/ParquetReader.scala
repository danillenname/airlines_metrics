package com.example
package readers

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}


class ParquetReader(spark: SparkSession) extends DataFrameReader {
  override def read(path: String, schema: StructType): DataFrame =
    spark.read
      .parquet(path)
}
