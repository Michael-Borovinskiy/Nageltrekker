package com.boro.apps.sqlops

import com.boro.apps.sqlops.DatesUtils.Period
import com.boro.apps.sqlops.DatesUtils.Period.{Month, Quarter, Year}
import com.boro.apps.sqlops.DfTransformer._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * @author Michael-Borovinskiy
 *         02.09.2024
 */


object AnalysisChecks {

  def checkCountRows(dfLeft: DataFrame, dfRight: DataFrame, seqCols: Seq[(String, String)]): Boolean = {

    val exprJoinKeys: Column = expr(seqCols.map(tuple => s"dfLeft.${tuple._1.trim} = dfRight.${tuple._2.trim}").mkString(" AND "))
    val dfAfterJoin = dfLeft.as("dfLeft").join(dfRight.as("dfRight"), exprJoinKeys, "full")

    (dfLeft.count == dfRight.count) && (dfLeft.count == dfAfterJoin.count)
  }

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

    val cols_inters = intersectColumns(df)
    val allRowsCnt: Long = df.count

    def putPercentage(value: Long): Long = {
      value * 100L / allRowsCnt
    }

    val dfResult = df.select(
      cols_inters.map(str => {
        lit(col(str + "_df1")).as(str + "_df1")
        }) ++
        cols_inters.map(str => {
          lit(col(str + "_df2")).as(str + "_df2")
        }) ++
        cols_inters.map(str => {
          lit(when(col(str + "_df1") === col(str + "_df2"), 1)).as(str + "_df1" + "=>-<=" + str + "_df2")
        }): _*
    )
    val aggrColumns = dfResult.columns diff df.columns.filter(_.endsWith("df1")).sorted diff df.columns.filter(_.endsWith("df2")).sorted

    val dfAggr = dfResult.select(
      aggrColumns.map(column => sum(col(column)).as(column)): _*
    )

    val map: Map[String, (Long, Long)] = dfAggr.collect.map(r => Map(dfAggr.columns.zip(r.toSeq): _*))
      .map(maptoMap =>
        maptoMap.flatMap(mm => Map(mm._1 -> (mm._2.asInstanceOf[Long], putPercentage(mm._2.asInstanceOf[Long])))))
      .apply(0)


    (dfResult, map)
  }

  def checkEqualColumnTypes(spark: SparkSession, df: DataFrame): CheckData = {

    import spark.implicits._

    val seqColTypes: Seq[(String, String)] = df.schema.map(stField => (stField.name, stField.dataType.toString))

    val df1 = seqColTypes.filter(tuple => tuple._1.endsWith("df1")).map(tuple => (tuple._1.dropRight(4), tuple._2)).toDF("cols_df1", "data_type_df1")
    val df2 = seqColTypes.filter(tuple => tuple._1.endsWith("df2")).map(tuple => (tuple._1.dropRight(4), tuple._2)).toDF("cols_df2", "data_type_df2")

    val dfRes = df1.as("df1").join(df2.as("df2"), $"df1.cols_df1" === $"df2.cols_df2", "full")

    val mapRes = dfRes.select(coalesce($"cols_df1", $"cols_df2").as("cols"), coalesce($"data_type_df1", lit("NaN")).as("data_type_df1"),
      coalesce($"data_type_df2", lit("NaN")).as("data_type_df2")).collect
      .flatMap(row => Map(row.getAs("cols").toString -> (row.getAs("data_type_df1").toString, row.getAs("data_type_df2").toString)))
      .toMap

    CheckData(dfRes, mapRes)
  }

  private def intersectColumns(df: DataFrame): Array[String] = {
    df.columns.filter(_.endsWith("df1")).map(_.dropRight(4)) intersect
      df.columns.filter(_.endsWith("df2")).map(_.dropRight(4))
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
