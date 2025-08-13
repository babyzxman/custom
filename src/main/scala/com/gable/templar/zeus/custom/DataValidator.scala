package com.gable.templar.zeus.custom

import com.gable.templar.heaven.exception.InvalidArgumentException
import com.gable.templar.zeus.service.vector.ConnectionInfo
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, expr}

import scala.util.Try
import scala.util.matching.Regex

object DataValidator {

  // Assumed SparkSession is available in the environment

  def validateTblOperation(jobName: String, schemaName: String, tblName: String, ruleCalc: String, ruleCalcApplyCol: List[String],
                           expectValue: Double, operation: String, sqlCalc: String = "",
                           sqlCalcApply: List[String] = List.empty, marginPct: Double = 0,
                           spark: SparkSession): Boolean = {

    println("Start validation")

    val lowerBound = expectValue - (expectValue * marginPct / 100)
    val upperBound = expectValue + (expectValue * marginPct / 100)

    val schemaTempTbl = TableUtils.tableSchemaCheck(tblName, schemaName)

    val dataframeTbl = if (sqlCalc.nonEmpty && sqlCalcApply.nonEmpty) {
      val sqlCalcWithTbl = sqlCalc.replace("tbl_name", schemaTempTbl)
      val formattedSqlCalc = sqlCalcApply.zipWithIndex.foldLeft(sqlCalcWithTbl) {
        case (currentSql, (value, index)) => currentSql.replace(s"{$index}", value)
      }
      println("function in sql replace")
      println(formattedSqlCalc)
      spark.sql(s"SELECT $formattedSqlCalc FROM $schemaTempTbl")
    } else {
      spark.sql(s"SELECT ${ruleCalcApplyCol.map(c => s"`$c`").mkString(", ")} FROM $schemaTempTbl")
    }

    println("DataFrame before aggregation:")
    dataframeTbl.show(5)

    // Regex to parse the aggregation rule
    val pattern: Regex = "(\\w+)\\((.*)\\)".r

    val dataframeTblRes = ruleCalc match {
      case pattern(aggType, columnExpr) =>
        println(s"Agg type: $aggType")

        // Use a match expression for aggregation functions
        val aggCol = aggType.toLowerCase match {
          case "sum" => functions.sum(expr(columnExpr))
          case "min" => functions.min(expr(columnExpr))
          case "max" => functions.max(expr(columnExpr))
          case "avg" => avg(expr(columnExpr))
          case _ => throw new InvalidArgumentException(s"Unsupported aggregation type: $aggType")
        }

        dataframeTbl.agg(aggCol.alias("validate_col"))

      case _ => // Handles cases where ruleCalc is not a function (e.g., "col_name")
        println("SELECT subquery only map result col")
        dataframeTbl.select(col(ruleCalcApplyCol.head).alias("validate_col"))
    }

    println("DataFrame after aggregation:")
    dataframeTblRes.show()

    val validateColValue = dataframeTblRes.collect().head.get(0) match {
      case d: java.math.BigDecimal => d.doubleValue() // Handle BigDecimal from Spark
      case d: Double => d
      case l: Long => l.toDouble
      case i: Int => i.toDouble
      case _ => throw new Exception("Unexpected data type for aggregated value")
    }

    println(s"Aggregated value: $validateColValue, Operation: $operation, Expected range: [$lowerBound, $upperBound], Acceptance margin percent: $marginPct")

    val isValid = operation match {
      case ">=" => validateColValue >= lowerBound
      case "<=" => validateColValue <= upperBound
      case "==" => validateColValue >= lowerBound && validateColValue <= upperBound
      case ">" => validateColValue > lowerBound
      case "<" => validateColValue < upperBound
      case _ => throw new InvalidArgumentException(s"Unsupported operation: $operation")
    }

    println(s"Validation result: $isValid")
    isValid
  }

  // Rewrite of `insert_to_target_mode`
  def insertToTargetMode(schemaName: String, tblName: String,
                         tmpValidationNm: String, insertMode: String,
                         partitionColumns: List[String] = List.empty,
                         uniqueKey: String = "", updateCondition: Option[String] = None,
                         jobNm: String = "",
                         connectionInfo: ConnectionInfo,spark:SparkSession,tmpz:String,
                         updateColumn: List[String] = List.empty): Unit = {
    // Rewrite of `check_table_type`
    def checkTableType(targetTableNm: String): String = {
      val queryTableType = s"""
        SELECT LOWER(name) as table_name, LOWER(source_TYPE) as source_type
        FROM public.datasource_profile
        WHERE LOWER(name) = LOWER('$targetTableNm')
      """
      val result = ConnectionService.postgresqlQueryDirectly(
        connectionInfo.getIp, connectionInfo.getPort, "hera",
        connectionInfo.getUserNm,connectionInfo.getPassword, queryTableType)
      try {
        while (result.rs.next()) {
          val tableType = result.rs.getString("source_type")
          return tableType
        }
      }
      finally{
        result.close()
      }
      throw new InvalidArgumentException("This target table doesn't exists in hera")
    }

    // Rewrite of `upsert_condition`
    def upsertCondition(tmpValidationDF: DataFrame, targetTblName: String, uniqueKey: String, mergeCondition: Option[String],
                        updateColumn: List[String],spark: SparkSession): Unit = {
      val deltaTable = DeltaTable.forName(spark, targetTblName)

      val uniqueKeyList = if (uniqueKey.startsWith("[") && uniqueKey.endsWith("]")) {
        uniqueKey.stripPrefix("[").stripSuffix("]").split(",").map(_.trim.stripPrefix("'").stripSuffix("'")).toList
      } else {
        List(uniqueKey)
      }

      val joinCondition = uniqueKeyList.map(key => s"trg.$key = src.$key").mkString(" AND ")
      val fullMergeCondition = mergeCondition.map(cond => s"$joinCondition AND $cond").getOrElse(joinCondition)

      val updateSetClause = if (updateColumn.nonEmpty) {
        updateColumn.filter(_ != "load_ts").map(c => (c, col(s"src.$c"))).toMap
      } else {
        tmpValidationDF.columns.filter(_ != "load_ts").map(c => (c, col(s"src.$c"))).toMap
      }

      val insertSetClause = tmpValidationDF.columns.map(c => (c, col(s"src.$c"))).toMap

      val mergeBuilder = deltaTable.as("trg").merge(tmpValidationDF.as("src"), fullMergeCondition)

      // Conditionally apply .whenMatched() based on whether there are columns to update
      val finalMergeStatement = if (updateSetClause.nonEmpty) {
        mergeBuilder.whenMatched().update(updateSetClause)
      } else {
        // If no update columns are specified, we just don't apply a matched update action.
        // This part of the code is implicitly handled by the next `.whenNotMatched` call.
        // We simply return the original mergeBuilder to continue chaining.
        mergeBuilder
      }

      finalMergeStatement
        .whenNotMatched()
        .insert(insertSetClause)
        .execute()
    }

    // Rewrite of `update_condition_func`
    def updateConditionFunc(tmpValidationDF: DataFrame, targetTblName: String, uniqueKey: String,
                            updateColumn: List[String], updateCondition: Option[String],
                            spark: SparkSession): Unit = {
      val deltaTable = DeltaTable.forName(spark, targetTblName)

      val uniqueKeyList = if (uniqueKey.startsWith("[") && uniqueKey.endsWith("]")) {
        uniqueKey.stripPrefix("[").stripSuffix("]").split(",").map(_.trim.stripPrefix("'").stripSuffix("'")).toList
      } else {
        List(uniqueKey)
      }

      val joinCondition = uniqueKeyList.map(key => s"trg.$key = src.$key").mkString(" AND ")
      val updateSetClause = updateColumn.map(c => (c, col(s"src.$c"))).toMap

      deltaTable.as("trg")
        .merge(tmpValidationDF.as("src"), joinCondition)
        .whenMatched(updateCondition.getOrElse("true"))
        .update(updateSetClause)
        .execute()
      println("Update update_condition_func Completed")
    }

    // Main logic starts here
    println("-- start insert --")

    val tmpTableNm = if (jobNm.nonEmpty) s"${jobNm}_$tmpValidationNm" else s"${tblName}_$tmpValidationNm"

    val schemaTargetTbl = TableUtils.tableSchemaCheck(tblName, schemaName)
    val schemaTempTbl = TableUtils.tableSchemaCheck(tmpTableNm, tmpz)
    var tempTableDf: DataFrame = null
    if(spark.catalog.tableExists(schemaTempTbl)) {
      spark.catalog.refreshTable(schemaTempTbl)
      tempTableDf = spark.table(schemaTempTbl)
    }
    else {
      val legacyTmpTableNm = s"${tblName}_$tmpValidationNm"
      val legacySchemaTempTbl = TableUtils.tableSchemaCheck(legacyTmpTableNm, tmpz)
      spark.catalog.refreshTable(legacySchemaTempTbl)
      tempTableDf = spark.table(legacySchemaTempTbl)
    }
    val tableType = checkTableType(schemaTargetTbl)

    insertMode.toLowerCase match {
      case "full_load" | "overwrite" if partitionColumns.isEmpty =>
        println("Full_load")
        val writer = tempTableDf.write.mode("overwrite")
        if (tableType.toLowerCase == "delta") writer.format("delta").saveAsTable(schemaTargetTbl)
        else writer.saveAsTable(schemaTargetTbl)

      case "overwrite" if partitionColumns.nonEmpty =>
        println("Overwrite with partition")
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        val writer = tempTableDf.write.mode("overwrite")
        if (tableType.toLowerCase == "delta") writer.format("delta").insertInto(schemaTargetTbl)
        else writer.insertInto(schemaTargetTbl)

      case "append" =>
        val writer = tempTableDf.write.mode("append")
        if (tableType.toLowerCase == "delta") writer.format("delta").insertInto(schemaTargetTbl)
        else writer.insertInto(schemaTargetTbl)

      case "upsert" =>
        upsertCondition(
          tmpValidationDF = tempTableDf,
          targetTblName = schemaTargetTbl,
          uniqueKey = uniqueKey,
          mergeCondition = updateCondition,
          updateColumn = updateColumn,
          spark
        )

      case "update" =>
        updateConditionFunc(
          tmpValidationDF = tempTableDf,
          targetTblName = schemaTargetTbl,
          uniqueKey = uniqueKey,
          updateColumn = updateColumn,
          updateCondition = updateCondition,
          spark
        )

      case _ =>
        throw new InvalidArgumentException(s"Unsupported insert mode: $insertMode")
    }

    println(s"Insert to target tbl: $schemaTargetTbl successful")
  }
}
