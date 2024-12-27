package com.boro.apps.sqlops

import com.boro.apps.sqlops.DatesUtils.Period
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

/**
 * @author Michael-Borovinskiy
 *         02.09.2024
 */
class AnalysisChecksTests extends munit.FunSuite {

  val spark: SparkSession = SparkSession.builder()
    .appName("spark_sql_operations")
    .master("local[6]")
    .getOrCreate()

  import spark.implicits._


  /**
   * Test DataFrames
   */
  val sq: DataFrame = spark.sql(
    """
      |SELECT 1 NUM, 'ANDREW' NAME, 10 EXPERIENCE, 900000 SALARY, 'ENGLAND' COUNTRY UNION ALL
      |SELECT 2 NUM, 'MARY' NAME, 9 EXPERIENCE, 350000 SALARY, 'USA' COUNTRY  UNION ALL
      |SELECT 3 NUM, 'ARNOLD' NAME, 3 EXPERIENCE, 400000 SALARY, 'ITALY' COUNTRY  UNION ALL
      |SELECT 4 NUM, 'HELEN' NAME, 8 EXPERIENCE, 500000 SALARY, 'USA' COUNTRY  UNION ALL
      |SELECT 5 NUM, 'WANE' NAME, 9 EXPERIENCE, 600000 SALARY, 'USA' COUNTRY  UNION ALL
      |SELECT 6 NUM, 'EDWARD' NAME, 10 EXPERIENCE, 900000 SALARY, 'USA' COUNTRY UNION ALL
      |SELECT 7 NUM, 'ANDREW' NAME, 10 EXPERIENCE, 900000 SALARY, 'FRANCE' COUNTRY UNION ALL
      |SELECT 8 NUM, 'EDWARD' NAME, 10 EXPERIENCE, 900000 SALARY, 'FRANCE' COUNTRY UNION ALL
      |SELECT 9 NUM, 'EDWARD' NAME, 10 EXPERIENCE, 900000 SALARY, 'USA' COUNTRY
      |""".stripMargin)

  val sq2: DataFrame = spark.sql(
    """
      |SELECT 1 NUM, 'ANDREW' NAME_STR, 10 EXPERIENCE_STR, 900000 SALARY UNION ALL
      |SELECT 2 NUM, 'MARY' NAME_STR, 8 EXPERIENCE_STR, 350000 SALARY  UNION ALL
      |SELECT 3 NUM, 'NIK' NAME_STR, 3 EXPERIENCE_STR, 200000 SALARY  UNION ALL
      |SELECT 4 NUM, 'BORIS' NAME_STR, 9 EXPERIENCE_STR, 500000 SALARY  UNION ALL
      |SELECT 5 NUM, 'WANE' NAME_STR, 11 EXPERIENCE_STR, 600000 SALARY  UNION ALL
      |SELECT 6 NUM, 'EDWARD' NAME_STR, 10 EXPERIENCE_STR, 900000 SALARY
      |""".stripMargin)

  val sq3: DataFrame = spark.sql(
    """
      |SELECT 1 NUM, 'ANDREW' NAME, 10 EXPERIENCE, 900000 SALARY UNION ALL
      |SELECT 2 NUM, 'MARY' NAME, 8 EXPERIENCE, 350000 SALARY  UNION ALL
      |SELECT 3 NUM, 'NIK' NAME, 3 EXPERIENCE, 400000 SALARY  UNION ALL
      |SELECT 4 NUM, 'BORIS' NAME, 9 EXPERIENCE, 500000 SALARY  UNION ALL
      |SELECT 5 NUM, 'WANE' NAME, 9 EXPERIENCE, 600000 SALARY  UNION ALL
      |SELECT 6 NUM, 'EDWARD' NAME, 10 EXPERIENCE, 900000 SALARY
      |""".stripMargin)

  val sq4: DataFrame = sq2.as("sq2").join(sq3.as("sq3"), Seq("NUM"), "full")
    .select(
      col("sq2.NAME_STR").as("NAME_df1"),
      col("sq2.EXPERIENCE_STR").as("EXPERIENCE_df1"),
      col("sq2.SALARY").as("SALARY_df1"),
      col("sq3.NAME").as("NAME_df2"),
      col("sq3.EXPERIENCE").as("EXPERIENCE_df2"),
      col("sq3.SALARY").as("SALARY_df2")
    )

  val sq5: DataFrame = sq2.as("sq2").join(sq3.as("sq3"), Seq("NUM"), "full")
    .select(
      col("sq2.NAME_STR").as("NAME_STR_df1"),
      col("sq2.EXPERIENCE_STR").as("EXPERIENCE_STR_df1"),
      col("sq2.SALARY").as("SALARY_df1"),
      col("sq3.NAME").as("NAME_df2"),
      col("sq3.EXPERIENCE").as("EXPERIENCE_df2"),
      col("sq3.SALARY").as("SALARY_df2")
    )

  val sq4difCntCols: DataFrame = sq2.as("sq2").join(sq3.as("sq3"), Seq("NUM"), "full")
    .select(
      col("NUM"),
      col("sq2.NAME_STR").as("NAME_df1"),
      col("sq2.EXPERIENCE_STR").as("EXPERIENCE_df1"),
      col("sq2.SALARY").as("SALARY_df1"),
      col("sq3.EXPERIENCE").as("EXPERIENCE_df2")
    )

  val sq4dif2CntCols: DataFrame = sq2.as("sq2").join(sq3.as("sq3"), Seq("NUM"), "full")
    .select(
      col("NUM"),
      col("sq2.NAME_STR").as("NAME_df1"),
      col("sq2.EXPERIENCE_STR").as("EXPERIENCE_df1"),
      col("sq2.SALARY").as("SALARY_df1"),
      col("sq3.SALARY").as("SALARY_df2")
    )

  val sqNodifCntCols: DataFrame = sq2.as("sq2").join(sq3.as("sq3"), Seq("NUM"), "full")
    .select(
      col("NUM"),
      col("sq2.NAME_STR").as("NAME_df1"),
      col("sq2.EXPERIENCE_STR").as("EXPERIENCE_df1"),
      col("sq2.SALARY").as("SALARY_df1")
    )

  val sqDt: DataFrame = spark.sql(
    """
      |SELECT 1 NUM, to_date("2023-10-03") gregor_dt UNION ALL
      |SELECT 2 NUM, to_date("2024-02-05") gregor_dt  UNION ALL
      |SELECT 4 NUM, to_date("2024-09-06") gregor_dt  UNION ALL
      |SELECT 5 NUM, to_date("2024-10-15") gregor_dt  UNION ALL
      |SELECT 6 NUM, to_date("2024-11-01") gregor_dt  UNION ALL
      |SELECT 7 NUM, to_date("2024-10-02") gregor_dt  UNION ALL
      |SELECT 8 NUM, to_date("2024-09-07") gregor_dt  UNION ALL
      |SELECT 9 NUM, to_date("2024-02-02") gregor_dt
      |""".stripMargin)

  test("checkCountRows gives unequal count for 2 DF, different count in src DF") {

    val isEqualCount = AnalysisChecks.checkCountRows(sq, sq2, Seq(("NUM","NUM")))
    assertNotEquals(isEqualCount, true)
  }

  test("checkCountRows gives equal count for 2 DF before and after join") {

    val isEqualCount = AnalysisChecks.checkCountRows(sq, sq, Seq(("NUM", "NUM")))
    assertEquals(isEqualCount, true)
  }

  test("countColumnValues check correct columns") {

    val df = AnalysisChecks.countColumnValues(sq2)


    assertEquals(df.select($"NUM_sum").as[String].collect()(0), "6")
    assertEquals(df.select($"EXPERIENCE_STR_sum").as[String].collect()(0), "5")
    assertEquals(df.select($"SALARY_sum").as[String].collect()(0), "5")
  }

  test("prepareDf check join count columns") {

    val df = AnalysisChecks.prepareDf(sq2, sq3, Seq("NUM"))

    assertEquals(df.columns.length, 7)
  }

  test("prepareDf check column_names") {

    val df = AnalysisChecks.prepareDf(sq2, sq3, Seq("NUM"))

    assertEquals(df.columns.count(_.contains("df1")), 3)
    assertEquals(df.columns.count(_.contains("df2")), 3)
  }


  test("checkEqualColumns returns correct results") {

    val res: CheckData = AnalysisChecks.checkEqualColumns(sq4)

    res.df.show(false)

    assertEquals(res.df.count(), 6L)
    assertEquals(res.mapResult.asInstanceOf[Map[String, (Long, Long)]]("EXPERIENCE_df1=>-<=EXPERIENCE_df2")._1, 5L)
    assertEquals(res.mapResult.asInstanceOf[Map[String, (Long, Long)]]("EXPERIENCE_df1=>-<=EXPERIENCE_df2")._2, 83L)

  }

  test("checkEqualColumns in different count columns dataframes") {

    val res: CheckData = AnalysisChecks.checkEqualColumns(sq4difCntCols)

    assertEquals(res.df.columns.length, 8)
    assertEquals(res.mapResult.keys.size, 1)
  }

  test("checkEqualColumns in different count columns dataframes dif range") {

    val res: CheckData = AnalysisChecks.checkEqualColumns(sq4dif2CntCols)

    assertEquals(res.df.columns.length, 8)
    assertEquals(res.mapResult.keys.size, 1)
  }


  test("checkEqualColumns no different count columns dataframes") {

    val res: CheckData = AnalysisChecks.checkEqualColumns(sqNodifCntCols)

    assertEquals(res.df.columns.length, 4)
    assertEquals(res.mapResult.keys.size, 0)
  }

  test("takeDiff returns exact count of rows") {

    val res: DataFrame = AnalysisChecks.takeDiff(sq2, sq3, Seq("NUM"))

    assertEquals(res.count, 1L)
  }

  test("checkEqualColumnTypes check count columns, key size in Map") {

    val res: CheckData = AnalysisChecks.checkEqualColumnTypes(spark, sq4)

    res.df.show(false)

    assertEquals(res.df.count(), 3L)
    assertEquals(res.mapResult.keys.size, 3)
  }


  test("checkEqualColumnTypes check count columns, key size in Map with different column naming") {

    val res: CheckData = AnalysisChecks.checkEqualColumnTypes(spark, sq5)

    res.df.show(false)
    res.mapResult.foreach(println)

    assertEquals(res.df.count(), 5L)
    assertEquals(res.mapResult.keys.size, 5)
  }


  test("find exact count of months") {

    val arr = AnalysisChecks.findNearestDates(sqDt, col("gregor_dt"),Period.Month)
    arr.foreach(println)
    assertEquals(arr.size, 5)
  }

  test("find exact count of quarters") {

    val arr = AnalysisChecks.findNearestDates(sqDt, col("gregor_dt"),Period.Quarter)
    arr.foreach(println)
    assertEquals(arr.size, 4)
  }

  test("find exact count of years") {

    val arr = AnalysisChecks.findNearestDates(sqDt, col("gregor_dt"),Period.Year)
    arr.foreach(println)
    assertEquals(arr.size, 2)
  }


  test("find nearest to first month dates") {

    val arr = AnalysisChecks.findNearestDates(sqDt, col("gregor_dt"),Period.Month)

    assertEquals(arr.sorted, Seq("2023-10-03", "2024-10-02", "2024-02-02", "2024-11-01", "2024-09-06").sorted)
  }

  test("find nearest to first quarters dates") {

    val arr = AnalysisChecks.findNearestDates(sqDt, col("gregor_dt"),Period.Quarter)

    assertEquals(arr.sorted, Seq("2024-02-02", "2023-10-03", "2024-09-06", "2024-10-02").sorted)
  }


  test("find nearest to first year dates") {

    val arr = AnalysisChecks.findNearestDates(sqDt, col("gregor_dt"),Period.Year)

    assertEquals(arr.sorted, Seq("2023-10-03", "2024-02-02").sorted)
  }


}

