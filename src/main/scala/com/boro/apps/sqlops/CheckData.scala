package com.boro.apps.sqlops

import org.apache.spark.sql.DataFrame

/**
 * @author Michael-Borovinskiy
 *         07.10.2024
 */
case class CheckData(
                    df: DataFrame, mapResult: Map[_,_]){}
