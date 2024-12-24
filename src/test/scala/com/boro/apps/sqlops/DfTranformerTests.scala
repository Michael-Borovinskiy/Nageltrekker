package com.boro.apps.sqlops

import org.apache.spark.sql.SparkSession

/**
 * @author Michael-Borovinskiy
 *         02.09.2024
 */
class DfTranformerTests extends munit.FunSuite {

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
      |SELECT 2 NUM, 'MARY' NAME, 9 EXPERIENCE, 350000 SALARY, 'USA' COUNTRY  UNION ALL
      |SELECT 3 NUM, 'ARNOLD' NAME, 3 EXPERIENCE, 400000 SALARY, 'ITALY' COUNTRY  UNION ALL
      |SELECT 4 NUM, 'HELEN' NAME, 8 EXPERIENCE, 500000 SALARY, 'USA' COUNTRY  UNION ALL
      |SELECT 5 NUM, 'WANE' NAME, 9 EXPERIENCE, 600000 SALARY, 'USA' COUNTRY  UNION ALL
      |SELECT 6 NUM, 'EDWARD' NAME, 10 EXPERIENCE, 900000 SALARY, 'USA' COUNTRY UNION ALL
      |SELECT 7 NUM, 'ANDREW' NAME, 10 EXPERIENCE, 900000 SALARY, 'FRANCE' COUNTRY UNION ALL
      |SELECT 8 NUM, 'EDWARD' NAME, 10 EXPERIENCE, 900000 SALARY, 'FRANCE' COUNTRY UNION ALL
      |SELECT 9 NUM, 'EDWARD' NAME, 10 EXPERIENCE, 900000 SALARY, 'USA' COUNTRY
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


  test("firstRowsInGroup count of rows") {

    val df = DfTransformer.firstRowsInGroup(sq, $"EXPERIENCE")($"NUM".desc)
    val count = df.count().toInt

    assertEquals(count, 4)
  }

  test("firstRowsInGroup count of rows: group for row") {

    val df = DfTransformer.firstRowsInGroup(sq, $"NUM")($"NUM".desc)
    val count = df.count().toInt

    assertEquals(count, 9)
  }

  test("firstRowsInGroup count of rows fail on incorrect".fail) {

    val df = DfTransformer.firstRowsInGroup(sq, $"EXPERIENCE")($"NUM".desc)
    val count = df.count().toInt

    assertEquals(count, 2)
  }

  test("firstRowsInGroup first record obtained") {

    val df = DfTransformer.firstRowsInGroup(sq, $"EXPERIENCE")($"NUM".desc)

    val str: Array[Int] = df.filter($"EXPERIENCE" === 10).select($"NUM").as[Int].collect()

    assertEquals(str.length,1)
    assertEquals(str(0), 9)
  }

  test("withCountRowColumnInGroup count column val equals") {

    val df = DfTransformer.withCountRowColumnInGroup(sq, "cnt", $"NAME")($"SALARY")

    val cnt: Array[String] = df.filter($"SALARY" === 900000).select($"cnt").as[String].collect()

    assertEquals(cnt(0).toLong, 5L)

  }

  test("withCountRowColumnInGroupFilterByCnt correct count and filter result") {

    val df = DfTransformer.withCountRowColumnInGroupFilterByCnt(sq, $"NAME", ">1")($"SALARY")

    assertEquals(df.count(), 5L)

  }

  test("withCollectSetDfValueOrdered correct collections result") {

    val df = DfTransformer.withCollectSetDfValueOrdered(sq, $"COUNTRY", "COUNTRIES_LST", $"NAME")($"SALARY".desc)

    val lst1: Array[String] = df.filter($"NAME" === "ANDREW").select($"COUNTRIES_LST").as[String].collect()
    val lst2: Array[String] = df.filter($"NAME" === "EDWARD").select($"COUNTRIES_LST").as[String].collect()

    assertEquals(lst1(0), "[FRANCE, ENGLAND]")
    assertEquals(lst2(0), "[FRANCE, USA]")

  }

  test("renameColsWithPrefSufWithExceptedCols correct collections result") {

    val df = DfTransformer.withCollectSetDfValueOrdered(sq, $"COUNTRY", "COUNTRIES_LST", $"NAME")($"SALARY".desc)

    val lst1: Array[String] = df.filter($"NAME" === "ANDREW").select($"COUNTRIES_LST").as[String].collect()
    val lst2: Array[String] = df.filter($"NAME" === "EDWARD").select($"COUNTRIES_LST").as[String].collect()

    assertEquals(lst1(0), "[FRANCE, ENGLAND]")
    assertEquals(lst2(0), "[FRANCE, USA]")

  }

  test("renameColumnsWithPrefSuf checks columns names after renaming") {

    val df = DfTransformer.renameColumnsWithPrefSuf(sq2, "grep", "_" , "rep")

    val colNames: Array[String] = df.columns

    assert(colNames.forall(_.startsWith("grep")))
    assert(colNames.forall(_.contains("_")))
    assert(colNames.forall(_.endsWith("rep")))

  }

  test("renameColsWithPrefSufWithExceptedCols checks columns names after renaming with excepted columns for renaming") {

    val df = DfTransformer.renameColsWithPrefSufWithExceptedCols(sq2, "grep", "_", "rep")("EXPERIENCE_STR", "NUM")

    val colNamesExcepted: Array[String] = df.select("EXPERIENCE_STR", "NUM").columns
    val colNamesNotExcepted: Array[String] = df.drop("EXPERIENCE_STR", "NUM").columns


    assert(colNamesNotExcepted.forall(_.startsWith("grep")))
    assert(colNamesNotExcepted.forall(_.contains("_")))
    assert(colNamesNotExcepted.forall(_.endsWith("rep")))

    assert(colNamesExcepted.contains(("EXPERIENCE_STR")))
    assert(colNamesExcepted.contains(("NUM")))
  }

}
