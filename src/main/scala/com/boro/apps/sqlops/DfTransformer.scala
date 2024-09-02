package com.boro.apps.sqlops

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window.partitionBy


object DfTransformer {

  def firstRowsInGroup(df: DataFrame, groups: Column*)(orders: Column*): DataFrame = {

    df.withColumn("rn", row_number().over(
      partitionBy(groups: _*).orderBy(orders: _*)))
      .filter(col("rn") === 1)
      .drop("rn")
  }


  def withCountRowColumnInGroup(df: DataFrame,
                                newCntColumnName: String,
                                column: Column)(groups: Column*): DataFrame = {

    df.withColumn(newCntColumnName, count(column).over(
      partitionBy(groups: _*)))
  }


  def withCountRowColumnInGroupFilterByCnt(df: DataFrame,
                                           column: Column,
                                           exprFilter: String
                                          )(groups: Column*): DataFrame = {

    df.withColumn("tmp_count", count(column).over(
      partitionBy(groups: _*)))
      .filter("tmp_count" + exprFilter).drop("tmp_count")
  }

  def withCollectSetDfValueOrdered(
                                    df: DataFrame,
                                    column: Column,
                                    columnName: String,
                                    groups: Column*
                                  )(orders: Column*): DataFrame = {
    val window = partitionBy(groups: _*).orderBy(orders: _*)
    df.withColumn(columnName, collect_set(column).over(window))
  }

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
