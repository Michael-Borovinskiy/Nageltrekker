package com.boro.apps.sqlops

import com.boro.apps.sqlops.DateUtils.Period
import com.boro.apps.sqlops.DateUtils.Period.{Month, Quarter, Year}
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

  def prepareDf(df1: DataFrame, df2: DataFrame, joinColNames: Seq[String]): DataFrame = {

    renameColsWithPrefSufWithExceptedCols(df1, "", "_", "df1")(joinColNames: _*)
      .join(
        renameColsWithPrefSufWithExceptedCols(df2, "", "_", "df2")(joinColNames: _*)
        , Seq(joinColNames: _*), "full")
  }

  def checkEqualColumns(df: DataFrame): (DataFrame, Map[String, (Long, Long)]) = {

    val col_df1 = df.columns.filter(_.endsWith("df1")).sorted
    val col_df2 = df.columns.filter(_.endsWith("df2")).sorted
    val arr = col_df1.zip(col_df2)

    val allRowsCnt: Long = df.count

    def putPercentage(value: Long): Long = {
      value * 100L / allRowsCnt
    }

    val dfResult = df.select(
      arr.map(tuple => {
        lit(col(tuple._1)).as(tuple._1)
      }) ++
        arr.map(tuple => {
          lit(col(tuple._2)).as(tuple._2)
        }) ++
        arr.map(tuple => {
          lit(when(col(tuple._1) === col(tuple._2), 1)).as(tuple._1 + "=>-<=" + tuple._2)
        }): _*
    )
    val aggrColumns = dfResult.columns diff col_df1 diff col_df2

    val dfAggr = dfResult.select(
      aggrColumns.map(column => sum(col(column)).as(column)): _*
    )

    val map: Map[String, (Long, Long)] = dfAggr.collect.map(r => Map(dfAggr.columns.zip(r.toSeq): _*))
      .map(mapMutable =>
        mapMutable.flatMap(mm => Map(mm._1 -> (mm._2.asInstanceOf[Long], putPercentage(mm._2.asInstanceOf[Long])))))
      .apply(0)


    (dfResult, map)
  }


  def findNearestDates(df: DataFrame, columnDt: Column, period: Period): Seq[String] = {

    val periodCase = period match {
      case Month => concat(month(columnDt), year(columnDt))
      case Quarter => date_trunc("quarter", columnDt)
      case Year => year(columnDt)
      case _ => columnDt
    }

    df.groupBy(
      periodCase.as("period")
    )
      .agg(min(columnDt).as(columnDt.toString()))
      .drop("period")
      .as[String](Encoders.STRING).collect()
  }

}
