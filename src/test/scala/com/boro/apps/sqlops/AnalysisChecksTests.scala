package com.boro.apps.sqlops

import org.apache.spark.sql.SparkSession

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
  val sq = spark.sql(
    """
      |SELECT 1 NUM, 'ANDREW' NAME, 10 EXPERIENCE, 900000 SALARY, 'ENGLAND' COUNTRY UNION ALL
      |SELECT 2 NUM, 'MARY' NAME, 9 HEIGHT, 350000 SALARY, 'USA' COUNTRY  UNION ALL
      |SELECT 3 NUM, 'ARNOLD' NAME, 3 HEIGHT, 400000 SALARY, 'ITALY' COUNTRY  UNION ALL
      |SELECT 4 NUM, 'HELEN' NAME, 8 HEIGHT, 500000 SALARY, 'USA' COUNTRY  UNION ALL
      |SELECT 5 NUM, 'WANE' NAME, 9 HEIGHT, 600000 SALARY, 'USA' COUNTRY  UNION ALL
      |SELECT 6 NUM, 'EDWARD' NAME, 10 HEIGHT, 900000 SALARY, 'USA' COUNTRY UNION ALL
      |SELECT 7 NUM, 'ANDREW' NAME, 10 HEIGHT, 900000 SALARY, 'FRANCE' COUNTRY UNION ALL
      |SELECT 8 NUM, 'EDWARD' NAME, 10 HEIGHT, 900000 SALARY, 'FRANCE' COUNTRY UNION ALL
      |SELECT 9 NUM, 'EDWARD' NAME, 10 HEIGHT, 900000 SALARY, 'USA' COUNTRY
      |""".stripMargin)

  val sq2 = spark.sql(
    """
      |SELECT 1 NUM, 'ANDREW' NAME_STR, 10 EXPERIENCE_STR, 900000 SALARY UNION ALL
      |SELECT 2 NUM, 'MARY' NAME_STR, 8 EXPERIENCE_STR, 350000 SALARY  UNION ALL
      |SELECT 3 NUM, 'NIK' NAME_STR, 3 EXPERIENCE_STR, 400000 SALARY  UNION ALL
      |SELECT 4 NUM, 'BORIS' NAME_STR, 9 EXPERIENCE_STR, 500000 SALARY  UNION ALL
      |SELECT 5 NUM, 'WANE' NAME_STR, 11 EXPERIENCE_STR, 600000 SALARY  UNION ALL
      |SELECT 6 NUM, 'EDWARD' NAME_STR, 10 EXPERIENCE_STR, 900000 SALARY
      |""".stripMargin)

  val sq3 = spark.sql(
    """
      |SELECT 1 NUM, 'ANDREW' NAME, 10 EXPERIENCE, 900000 SALARY UNION ALL
      |SELECT 2 NUM, 'MARY' NAME, 8 EXPERIENCE, 350000 SALARY  UNION ALL
      |SELECT 3 NUM, 'NIK' NAME, 3 EXPERIENCE, 400000 SALARY  UNION ALL
      |SELECT 4 NUM, 'BORIS' NAME, 9 EXPERIENCE, 500000 SALARY  UNION ALL
      |SELECT 5 NUM, 'WANE' NAME, 11 EXPERIENCE, 600000 SALARY  UNION ALL
      |SELECT 6 NUM, 'EDWARD' NAME, 10 EXPERIENCE, 900000 SALARY
      |""".stripMargin)


  test("countColumnValues check correct columns") {

    val df = AnalysisChecks.countColumnValues(sq2)


    assertEquals(df.select($"NUM_sum").as[String].collect()(0), "6")
    assertEquals(df.select($"EXPERIENCE_STR_sum").as[String].collect()(0), "5")
    assertEquals(df.select($"SALARY_sum").as[String].collect()(0), "5")
  }

  test("prepareDf check join count columns") {

    val df = AnalysisChecks.prepareDf(sq2, sq3, "NUM")

    assertEquals(df.columns.length, 7)
  }

  test("prepareDf check column_names") {

    val df = AnalysisChecks.prepareDf(sq2, sq3, "NUM")

    assertEquals(df.columns.count(_.contains("df1")), 3)
    assertEquals(df.columns.count(_.contains("df2")), 3)
  }


}
