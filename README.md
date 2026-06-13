# Nageltrekker

**A Scala library for data analysis and validation on Apache Spark**

**Nageltrekker** is a Scala library providing a set of utilities for data quality checks on Spark DataFrames: dataset comparison (by rows, columns, data types), window-function-based transformations, and date handling. Designed for use in ETL pipelines and Data Quality (DQ) processes.

## Technology Stack

| Component       | Version       |
|-----------------|--------------|
| Java            | 1.8          |
| Scala           | 2.12.11      |
| Apache Spark    | 2.4.5        |
| MUnit (tests)   | 0.7.15       |
| Maven           | (scala-maven-plugin 4.8.1) |

---

## Module Description

### 1. `DatesUtils` — Date Handling

Contains an abstract class `Period` and three constant objects:
- `Month` — grouping by month (month + year)
- `Quarter` — grouping by quarter
- `Year` — grouping by year

Used by `findNearestDates` in `AnalysisChecks` to determine period boundaries when searching for nearest dates.

### 2. `DfTransformer` — DataFrame Transformations

A set of static methods for Spark DataFrame transformations:

| Method | Description |
|-------|----------|
| `firstRowsInGroup` | Selects the first row in each group, ordered by specified columns (via `row_number()` over a window) |
| `withCountRowColumnInGroup` | Adds a column with the row count within a group (via `count()` over a window) |
| `withCountRowColumnInGroupFilterByCnt` | Same, but with subsequent group filtering by a condition |
| `withCollectSetDfValueOrdered` | Adds a column with a set of unique values in a group (via `collect_set()` over an ordered window) |
| `renameColumnsWithPrefSuf` | Batch column renaming: adds a prefix and/or suffix to all columns |
| `renameColsWithPrefSufWithExceptedCols` | Same, but allows excluding specific columns from renaming |

### 3. `CheckData` — Check Result Container

A case class combining:
- `df: DataFrame` — DataFrame with check results
- `mapResult: Map[_, _]` — Map representation of results for convenient programmatic access

Used by `checkEqualColumns` and `checkEqualColumnTypes` to return detailed results.

### 4. `AnalysisChecks` — Analytical Checks

The main module of the library, implementing data quality checks:

| Method | Description |
|-------|----------|
| `checkCountRows` | Verifies row count equality between two DataFrames before and after a full outer join on given keys |
| `difColumns` | Returns a Map with columns present only in the left dataset (`leftNewCols`) and only in the right dataset (`rightNewCols`) |
| `takeDiffOnEqualColumnsByValue` | Compares values of identically named columns from two datasets; returns rows with discrepancies |
| `countColumnValues` | Returns the number of distinct values (`countDistinct`) for each column |
| `prepareDf` | Prepares two datasets for comparison: renames columns with `_df1`/`_df2` suffixes and performs a full outer join |
| `checkEqualColumns` | For each pair of identically named columns (`col_df1`, `col_df2`), computes the count and percentage of matching values; returns `CheckData` |
| `checkEqualColumnTypes` | Compares data types of identically named columns; returns `CheckData` with mapping results |
| `findNearestDates` | Finds the nearest dates in the specified period (month/quarter/year) for a given date column |

---

## Module Relationships

```
AnalysisChecks
  ├── calls DfTransformer.renameColsWithPrefSufWithExceptedCols (prepareDf)
  ├── uses DatesUtils.Period (findNearestDates)
  └── returns CheckData (checkEqualColumns, checkEqualColumnTypes)
```

---

## Build & Test

```bash
# Build the project
mvn clean package

# Run tests
mvn test

# Build JAR
mvn package
```

### Test Specifics

- Tests use a local SparkSession with `master("local[6]")`
- Testing framework — MUnit (`munit.FunSuite`)
- Coverage includes: dataset comparison, diff search, date operations, DataFrame transformations
- Tests depend on a properly configured Spark environment

---

## Usage Example

```scala
import com.boro.apps.sqlops._
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().master("local[*]").getOrCreate()
import spark.implicits._

// Create test data
val df1 = Seq(
  (1, "Alice", 1000),
  (2, "Bob",   2000)
).toDF("id", "name", "salary")

val df2 = Seq(
  (1, "Alice", 1000),
  (2, "Robert", 2500)
).toDF("id", "name", "salary")

// Check value differences
val diffs = AnalysisChecks.takeDiffOnEqualColumnsByValue(df1, df2, Seq("id"))
diffs.show()

// Compare columns
val colsDiff = AnalysisChecks.difColumns(df1, df2)
println(s"New columns on left: ${colsDiff("leftNewCols")}")
println(s"New columns on right: ${colsDiff("rightNewCols")}")

// Check row count
val isEqual = AnalysisChecks.checkCountRows(df1, df2, Seq(("id", "id")))
println(s"Row counts match: $isEqual")
```

---

## License

This project is distributed under the license specified in the repository.

## Author

**Michael-Borovinskiy**