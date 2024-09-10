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

  def prepareDf(df1: DataFrame, df2: DataFrame, joinColNames: String*): DataFrame = {

    renameColsWithPrefSufWithExceptedCols(df1, "", "_", "df1")(joinColNames: _*)
      .join(
        renameColsWithPrefSufWithExceptedCols(df2, "", "_", "df2")(joinColNames: _*)
        , Seq(joinColNames: _*), "full")
  }

  def checkEqualColumns(df: DataFrame):(DataFrame, Map[String,(Long, Long)])  = {

   val col_df1 =  df.columns.filter(_.endsWith("df1")).sorted
   val col_df2 =  df.columns.filter(_.endsWith("df2")).sorted

   val arr =  col_df1.zip(col_df2)

   val dfResult =  df.select(
     arr.map(tuple => {
       lit(col(tuple._1)).as(tuple._1)}) ++
     arr.map(tuple => {
         lit(col(tuple._2)).as(tuple._2)}) ++
     arr.map(tuple => {
       lit(col(tuple._1) === col(tuple._2)).as(tuple._1 + "_" + tuple._2)}):_*
   )

    dfResult.show(30, false) // --

    (dfResult, Map("one" -> (1L,2L))) //TODO Map implement
  }


  def findNearestDates(df: DataFrame, columnDt: Column, period: Period): Seq[String] = {

    val periodCase = period match {
      case Month => concat(month(columnDt),year(columnDt))
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
