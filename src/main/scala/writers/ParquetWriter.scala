package com.example
package writers

import readers.ParquetReader

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.io.File

class ParquetWriter(spark: SparkSession) {

  def writeParquetTable(path: String, mode: SaveMode, schema: StructType)(df: DataFrame): Unit = {
    mode match {
      case SaveMode.Append => {
        df.coalesce(1)
          .write
          .mode(mode)
          .parquet(path)
      }

      case SaveMode.Overwrite => {
        val parquetReader = new ParquetReader(spark)

        df.coalesce(1)
          .write
          .mode(mode)
          .parquet(path + "_tmp")

        val tmpTable = parquetReader.read(path + "_tmp", schema)

        tmpTable.coalesce(1)
          .write
          .mode(mode)
          .parquet(path)

        FileUtils.deleteQuietly(new File(path + "_tmp"))
      }


    }
  }
}
