package com.boro.apps.sqlops

/**
 * @author Michael-Borovinskiy
 *         09.09.2024
 */

/**
 * The DatesUtils object contains classes and objects for proccessing with dates, used for implementation in methods for checks with dates
 */
object DatesUtils {
  abstract class Period(val name: String)

  object Period {
    case object Month extends Period("Month")

    case object Quarter extends Period("Quarter")

    case object Year extends Period("Year")
  }

}
