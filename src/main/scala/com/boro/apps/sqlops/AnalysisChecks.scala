package com.boro.apps.sqlops

import com.boro.apps.sqlops.DatesUtils.Period
import com.boro.apps.sqlops.DatesUtils.Period.{Month, Quarter, Year}
import com.boro.apps.sqlops.DfTransformer._
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.functions.{col, _}

/**
 * @author Michael-Borovinskiy
 *         02.09.2024
 */


/**
 * The AnalysisChecks class contains methods for resolving data checks in DataFrames
 */
object AnalysisChecks {

  /**
   *
   * @param dfLeft - spark.sql.DataFrame 1 for comparing
   * @param dfRight - spark.sql.DataFrame 2 for comparing
   * @param seqCols - join columns sequence of tuples with column names from (@param dfLeft, @param dfRight)
   * @return Check (Boolean) of rows quantity equality for two comparable DataFrames and after its joined by @param seqCols
   */
  def checkCountRows(dfLeft: DataFrame, dfRight: DataFrame, seqCols: Seq[(String, String)]): Boolean = {

    val exprJoinKeys: Column = expr(seqCols.map(tuple => s"dfLeft.${tuple._1.trim} = dfRight.${tuple._2.trim}").mkString(" AND "))
    val dfAfterJoin = dfLeft.as("dfLeft").join(dfRight.as("dfRight"), exprJoinKeys, "full")

    (dfLeft.count == dfRight.count) && (dfLeft.count == dfAfterJoin.count)
  }

  /**
   *
   * @param dfLeft - spark.sql.DataFrame 1 for comparing
   * @param dfRight - spark.sql.DataFrame 2 for comparing
   * @return Map with sequences of new columns for each of comparable DataFrame
   */
  def difColumns(dfLeft: DataFrame, dfRight: DataFrame): Map[String, Seq[String]] = {
    Map("leftNewCols" -> (dfLeft.columns diff dfRight.columns).toSeq,
      "rightNewCols" -> (dfRight.columns diff dfLeft.columns).toSeq)
  }

  /**
   *
   * @param dfLeft  - spark.sql.DataFrame 1 for comparing
   * @param dfRight - spark.sql.DataFrame 2 for comparing
   * @param joinColNames - join columns sequence of column names to join two dataframes before taking diff
   * @return spark.sql.DataFrame with different column values for compared @param dfLeft and @param dfRight
   */
  def takeDiffOnEqualColumnsByName(dfLeft: DataFrame, dfRight: DataFrame, joinColNames: Seq[String]): DataFrame = {
    val preparedDf = prepareDf(dfLeft, dfRight, joinColNames)
    val dfResult = mergeColumnsWithStat(preparedDf)

    val filterCols = dfResult.columns.filter(_.contains("=>-<="))

    dfResult.withColumn("checkDiff", when(filterCols.map(col).reduce(_+_).isNull, true).otherwise(false))
    .filter(col("checkDiff"))

  }

  /**
   *
   * @param df - spark.sql.DataFrame
   * @return spark.sql.DataFrame with distinct count for each column in @param df
   */
  def countColumnValues(df: DataFrame): DataFrame = {
    val namesCols = df.columns.map(colName => colName + "_sum")

    df.select(
      df.columns.map(cols => countDistinct(col(cols))): _*)
      .toDF(namesCols: _*)
  }

  /**
   *
   * @param dfLeft - spark.sql.DataFrame 1 for comparing
   * @param dfRight - spark.sql.DataFrame 2 for comparing
   * @param joinColNames - join columns sequence of column names
   * @return spark.sql.DataFrame after join of DataFrames with columns renaming
   */
  def prepareDf(dfLeft: DataFrame, dfRight: DataFrame, joinColNames: Seq[String]): DataFrame = {

    renameColsWithPrefSufWithExceptedCols(dfLeft, "", "_", "df1")(joinColNames: _*)
      .join(
        renameColsWithPrefSufWithExceptedCols(dfRight, "", "_", "df2")(joinColNames: _*)
        , Seq(joinColNames: _*), "full")
  }

  /**
   *
   * @param df - spark.sql.DataFrame
   * @return CheckData with df: DataFrame with calculation of equal column and percentage of equal values in each column, mapResult: Map with results of calculation
   */
  def checkEqualColumns(df: DataFrame): CheckData = {

    val df_proc = mergeColumnsWithStat(df)
    val allRowsCnt: Long = df.count

    def putPercentage(value: Long): Long = {
      value * 100L / allRowsCnt
    }

    val aggrColumns = df_proc.columns diff df.columns.filter(_.endsWith("df1")).sorted diff df.columns.filter(_.endsWith("df2")).sorted diff df.columns

    val dfAggr = df_proc.select(
      aggrColumns.map(column => sum(col(column)).as(column)): _*
    )

    val map: Map[String, (Long, Long)] = dfAggr.collect.map(r => Map(dfAggr.columns.zip(r.toSeq): _*))
      .map(maptoMap =>
        maptoMap.flatMap(mm => Map(mm._1 -> (mm._2.asInstanceOf[Long], putPercentage(mm._2.asInstanceOf[Long])))))
      .apply(0)

    CheckData(df_proc, map)
  }

  /**
   *
   * @param df - spark.sql.DataFrame
   * @return spark.sql.DataFrame with statistics after comparing values of Dataframe
   */
  private def mergeColumnsWithStat(df: DataFrame):DataFrame = {


    val cols_inters = intersectColumns(df)
    val dfCols = df.columns diff cols_inters

      df.select( dfCols.map(col) ++
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
  }

  /**
   *
   * @param spark - SparkSession
   * @param df - spark.sql.DataFrame
   * @return CheckData with df: DataFrame with calculation of equal column types from df.schema, mapResult: Map with results of calculation
   */
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

  /**
   * @return intersection of column names in Dataframe. Used in intermediate level of calculations
   */
  private def intersectColumns(df: DataFrame): Array[String] = {
    df.columns.filter(_.endsWith("df1")).map(_.dropRight(4)) intersect
      df.columns.filter(_.endsWith("df2")).map(_.dropRight(4))
  }


  /**
   *
   * @param df - spark.sql.DataFrame
   * @param columnDt - spark.sql.Column which is used for calculation
   * @param period - sqlops.DatesUtils.Period for defining period constraint
   * @return sequence of dates (type: string) defined in @param period
   */
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
