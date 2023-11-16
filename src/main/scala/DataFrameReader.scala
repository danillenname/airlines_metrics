package com.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait DataFrameReader {
  def read(path: String, schema: StructType): DataFrame

}
