package com.boro.apps.sqlops

import com.boro.apps.sqlops.DfTransformer._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * @author Michael-Borovinskiy
 *         02.09.2024
 */



object AnalysisChecks {

  def difColumns(df: DataFrame, df2: DataFrame): Map[String, Seq[String]] = {
    Map("leftNewCols" -> (df.columns diff df2.columns).toSeq,
      "rightNewCols" -> (df2.columns diff df.columns).toSeq)
  }

  def countColumnValues(df: DataFrame): DataFrame = {
    val namesCols = df.columns.map(colName => colName + "_sum")

    df.select(
      df.columns.map(cols => countDistinct(col(cols))): _*)
      .toDF(namesCols: _*)
  }

  // prepare DF
  def prepareDf(df1: DataFrame, df2: DataFrame, joinColNames: String*): DataFrame = {

    renameColsWithPrefSufWithExceptedCols(df1, "", "_", "df1")(joinColNames: _*)
      .join(
        renameColsWithPrefSufWithExceptedCols(df2, "", "_", "df2")(joinColNames: _*)
        , Seq(joinColNames: _*), "full")
  }
}
