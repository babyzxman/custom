package com.gable.templar.zeus.custom

import com.gable.templar.heaven.exception.InvalidArgumentException
import com.gable.templar.zeus.controller.tablemanage.view.PartitionCondition
import com.gable.templar.zeus.util.ComparatorUtil

import java.util.regex.Pattern

object PartitionParsers {

  // Order matters: longer operators first
  private val OpRegex = ">=|<=|<>|!=|=|>|<"
  private val Cond: Pattern =
    Pattern.compile(s"""^\\s*([A-Za-z_][A-Za-z0-9_\\.]*)\\s*($OpRegex)\\s*(.+?)\\s*$$""")


  /** Parses strings like:
   *  "ictrl_dt = '{ICTRL_DT}' and src_sys = 'Topping' and version >= 5"
   * Root has implicit AND; leaves contain name/comparator/value(+dynamic)
   */
  def parseAnd(expr: String): List[PartitionCondition] = {
    var partitionConditionList: List[PartitionCondition] = List.empty

    // Split on AND (case-insensitive) with surrounding whitespace
    val parts = expr.split("(?i)\\s+and\\s+").map(_.trim).filter(_.nonEmpty)

    parts.foreach { part =>
      val m = Cond.matcher(part)
      if (m.find()) {
        val name       = m.group(1)
        val comparator = m.group(2)
        val rawValue   = m.group(3)
        val value      = stripQuotes(rawValue)
        if(name != "ictrl_dt") {
          val dynamic = containsPlaceholder(rawValue) || isBarePlaceholder(value)
          val leaf = new PartitionCondition(name, value,convertComparator(comparator))
          leaf.setDynamic(java.lang.Boolean.valueOf(dynamic))
          partitionConditionList = partitionConditionList :+ leaf
        }
      }
    }
    partitionConditionList
  }

  private def stripQuotes(s: String): String = {
    if (s == null) null
    else {
      val t = s.trim
      if ((t.startsWith("'") && t.endsWith("'")) || (t.startsWith("\"") && t.endsWith("\"")))
        t.substring(1, t.length - 1)
      else t
    }
  }

  def convertComparator(symbol: String): String = {
    symbol match {
      case "=" => {
        "eq"
      }
      case "<" => {
        "lt"
      }
      case "<=" => {
        "lte"
      }
      case ">" => {
        "gt"
      }
      case ">=" => {
        "gte"
      }
      case _ => {
        throw new InvalidArgumentException("Comparator is not supported")
      }
    }
  }

  def convertComparatorToSymbol(comparator: String): String = {
    comparator match {
      case "eq" => {
        "="
      }
      case "lt" => {
        "<"
      }
      case "lte" => {
        "lte"
      }
      case "gt" => {
        ">"
      }
      case "gte" => {
        ">="
      }
      case _ => {
        throw new InvalidArgumentException("Comparator is not supported")
      }
    }
  }

  private def containsPlaceholder(s: String): Boolean =
    s != null && s.contains("{") && s.contains("}")

  private def isBarePlaceholder(s: String): Boolean =
    s != null && s.matches("\\{[^}]+}")
}
