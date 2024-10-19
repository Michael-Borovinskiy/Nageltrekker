package com.boro.apps.sqlops

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window.partitionBy


/**
 * The DfTransformer class contains methods for transforming data in DataFrames
 */
object DfTransformer {

  /**
   *
   * @param df - spark.sql.DataFrame
   * @param groups - vararg spark.sql.Column for group by operation
   * @param orders - vararg spark.sql.Column for order by operation
   * @return spark.sql.DataFrame with only first row in @param groups, ordered by @param orders
   */
  def firstRowsInGroup(df: DataFrame, groups: Column*)(orders: Column*): DataFrame = {

    df.withColumn("rn", row_number().over(
      partitionBy(groups: _*).orderBy(orders: _*)))
      .filter(col("rn") === 1)
      .drop("rn")
  }


  /**
   *
   * @param df - spark.sql.DataFrame
   * @param newCntColumnName - new column name arg String
   * @param column - spark.sql.Column, used for count rows in group
   * @param groups - vararg spark.sql.Column for group by operation
   * @return spark.sql.DataFrame, supplemented with a count rows in group column
   */
  def withCountRowColumnInGroup(df: DataFrame,
                                newCntColumnName: String,
                                column: Column)(groups: Column*): DataFrame = {

    df.withColumn(newCntColumnName, count(column).over(
      partitionBy(groups: _*)))
  }


  /**
   *
   * @param df - spark.sql.DataFrame
   * @param column - spark.sql.Column, used for count rows in group
   * @param exprFilter - expression for filter, e.g. ">1" for filter
   * @param groups - vararg spark.sql.Column for group by operation
   * @return spark.sql.DataFrame, supplemented with a count rows in group column and filtered by @param exprFilter
   */
  def withCountRowColumnInGroupFilterByCnt(df: DataFrame,
                                           column: Column,
                                           exprFilter: String
                                          )(groups: Column*): DataFrame = {

    df.withColumn("tmp_count", count(column).over(
      partitionBy(groups: _*)))
      .filter("tmp_count" + exprFilter).drop("tmp_count")
  }


  /**
   *
   * @param df - spark.sql.DataFrame
   * @param column - spark.sql.Column, used for collect_set
   * @param columnName - new column name arg String
   * @param groups - vararg spark.sql.Column for group by operation
   * @param orders - vararg spark.sql.Column for order by operation
   * @return spark.sql.DataFrame, supplemented with a set of values in window column
   */
  def withCollectSetDfValueOrdered(
                                    df: DataFrame,
                                    column: Column,
                                    columnName: String,
                                    groups: Column*
                                  )(orders: Column*): DataFrame = {
    val window = partitionBy(groups: _*).orderBy(orders: _*)
    df.withColumn(columnName, collect_set(column).over(window))
  }

  /**
   *
   * @param df - spark.sql.DataFrame
   * @param prefix - part of column name which goes before the main part of column name
   * @param separator - separator between parts of column name
   * @param suffix - part of column name which goes after the main part of column name
   * @return spark.sql.DataFrame with renamed columns from args structure
   */
  def renameColumnsWithPrefSuf(df: DataFrame, prefix: String, separator: String, suffix: String): DataFrame = {
    val namesCols = df.columns.map(colName => {
      (if (prefix == "") {
        ""
      } else {
        prefix + separator
      }) + colName + (if (suffix == "") {
        ""
      } else {
        separator + suffix
      })
    })

    df.select(df.columns.map(cols => col(cols)): _*)
      .toDF(namesCols: _*)
  }

  /**
   *
   * @param df - spark.sql.DataFrame
   * @param prefix - part of column name which goes before the main part of column name
   * @param separator - separator between parts of column name
   * @param suffix - part of column name which goes after the main part of column name
   * @param colNamesExcepted - column names String for excluding from columns marked to rename
   * @return spark.sql.DataFrame with renamed columns from args structure, @param colNamesExcepted without renaming
   */
  def renameColsWithPrefSufWithExceptedCols(df: DataFrame,
                                            prefix: String,
                                            separator: String,
                                            suffix: String
                                           )(colNamesExcepted: String*): DataFrame = {
    val namesCols = df.columns.map(colName => {

      val transformed =
        (if (prefix == "") {
          ""
        } else {
          prefix + separator
        }) + colName + (if (suffix == "") {
          ""
        } else {
          separator + suffix
        })

      if (colNamesExcepted.contains(colName)) {
        colName
      } else transformed
    })

    df.select(df.columns.map(cols => col(cols)): _*)
      .toDF(namesCols: _*)
  }


}
