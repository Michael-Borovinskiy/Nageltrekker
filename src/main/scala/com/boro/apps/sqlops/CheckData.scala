package com.boro.apps.sqlops

import org.apache.spark.sql.DataFrame

/**
 * @author Michael-Borovinskiy
 *         07.10.2024
 */

/**
 * The CheckData case class is used as a form of unified returned value for analytical checks. It contains DataFrame with columns stored results of checks, Map with checks
 *
 * @param df - spark.sql.DataFrame
 * @param mapResult - Map with results from checks calculations
 */
case class CheckData(
                    df: DataFrame, mapResult: Map[_,_]){}
