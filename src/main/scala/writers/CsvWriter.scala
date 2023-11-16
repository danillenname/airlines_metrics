package com.example
package writers

import com.github.tototoshi.csv.CSVWriter
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.io.File
import java.nio.file.{Files, Paths}

object CsvWriter {
  case class Config(hasHeader: Boolean = true)
}

class CsvWriter(config: CsvWriter.Config) {
  private def getFile(path: String) = new File(path)

  def createCsvTable(path: String, cols: Array[String]): Unit = {
    val f = getFile(path)

    if (Files.notExists(Paths.get(path))) {
      val writer = CSVWriter.open(f)
      writer.writeAll(Seq(Seq(cols: _*)))
    }
  }

  def writeMeta(minDate: String, maxDate: String, processed: String, path: String): Unit = {
    val f = getFile(path)

    val writer = CSVWriter.open(f, append = true)
    writer.writeAll(Seq(Seq(minDate, maxDate, processed)))

  }

  def writeCsvTable(path: String, mode: SaveMode)(df: DataFrame): Unit = {
    df.coalesce(1)
      .write
      .mode(mode)
      .option("header", config.hasHeader)
      .csv(path)
  }
}
