package com.gable.templar.zeus.custom

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.gable.templar.heaven.exception.InvalidArgumentException
import com.gable.templar.zeus.SparkServer
import com.gable.templar.zeus.controller.tablemanage.view.PartitionCondition
import com.gable.templar.zeus.service.spark.PartitionKey
import com.gable.templar.zeus.service.spark.TableManageService.PATH_SEPERATOR
import com.gable.templar.zeus.service.vector.ConnectionInfo
import io.delta.tables.DeltaTable
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, current_timestamp, expr, lit}
import org.apache.spark.sql.types.IntegerType
import org.slf4j.LoggerFactory

import java.sql.ResultSet
import java.util.UUID
import scala.util.Try
import scala.util.matching.Regex

object DataValidator {

  private val logger = LoggerFactory.getLogger(DataValidator.getClass)

  val scalaObjectMapper: ObjectMapper = new ObjectMapper()

  scalaObjectMapper.registerModule(DefaultScalaModule)


  private val autoGenerateColName: Set[String] = Set("execution_id","process_name","dw_last_update_time")

  // Assumed SparkSession is available in the environment

  def parseListString(s: String): List[String] = {
    if (s == null || s.trim.isEmpty) {
      List.empty[String]
    } else {
      s.stripPrefix("[").stripSuffix("]").split(",").map(_.trim.stripPrefix("'").stripSuffix("'")).toList
    }
  }

  def convertToPartitionSql(partitionStr: String): String = {
    val partitions = partitionStr.split("/")
    partitions.map(p => p.split("=")(0) + "='" + p.split("=")(1) + "'").mkString(",")
  }

  def deletePartition(path: String,
                      tableName: String,sparkSession: SparkSession,
                      dropPartitionList: List[String]): Unit = {
    dropPartitionList.foreach(partToDrop => {
      //				zeusSession.sql(s"ALTER table ${tableName} DROP IF EXISTS partition (${partToDrop.replaceAll("/", ",")})")
      sparkSession.sql(s"ALTER table ${tableName} DROP IF EXISTS partition (${convertToPartitionSql(partToDrop)})")
    })
    val deletedSrcPaths = dropPartitionList.map(partToDrop => path + PATH_SEPERATOR + partToDrop)
    deletedSrcPaths.foreach(p => {
      SparkServer.getStoreUtil().delete(p,true)
    })
  }

  def convertToDeleteCondition(partitionStr: String,alias: String): String = {
    val partitions = partitionStr.split("/")
    partitions.map { p =>
      val Array(col, value) = p.split("=")
      s"$alias.$col='$value'"
    }.mkString(" AND ")
  }

  def generateWhereConditionFromDeletePartition(deletePartitions: List[String],alias: String): String = {
    if(deletePartitions.nonEmpty) {
      val sb = new StringBuilder()
      var count = 0
      for (partitionToDelete <- deletePartitions) {
        if(!partitionToDelete.equals("ictrl_dt")) {
          sb.append(f" (${convertToDeleteCondition(partitionToDelete, alias)})")
          if (count < deletePartitions.size - 1) {
            sb.append(" OR ")
          }
        }
        count += 1
      }
      return sb.toString()
    }
    ""
  }

  def generateWhereConditionFromPartitionCondition(partitionConditionList: List[PartitionCondition]): String = {
    if(partitionConditionList.nonEmpty) {
      val sb = new StringBuilder()
      sb.append("WHERE ")
      var count = 0
      partitionConditionList.foreach(f => {
        sb.append(f" ${f.getName} ${PartitionParsers.convertComparatorToSymbol(f.getComparator)} ${f.getValue}")
        if (count < partitionConditionList.size - 1) {
          sb.append(" AND ")
        }
        count += 1
      })
      sb.toString()
    }
    else {
      ""
    }
  }

  def processValidationRows(spark: SparkSession,
                             validationDf: ResultSet,
                             tmpValidationNm: String,
                             loadType: String,
                             whereCondition: String
                           ): Map[String, Boolean] = {

    var validationCountSeq = 1
    val resultDict = scala.collection.mutable.Map[String, Boolean]()

    while (validationDf.next()) {
      println(s"Start validate on seq : $validationCountSeq")

      val vdJobName = validationDf.getString("job_nm")
      val vdSchemaName = validationDf.getString("schema_nm")
      val vdTableName = validationDf.getString("tbl_nm")
      val vdRuleName = validationDf.getString("rule_nm")
      val tmpVdTableName = s"${vdTableName}_$tmpValidationNm"
      val vdRuleCalc = validationDf.getString("rule_calc")

      val vdRuleCalcApplyColString = validationDf.getString("rule_calc_apply_col")
      val vdRuleCalcApplyCol: List[String] = parseListString(vdRuleCalcApplyColString)

      val vdExpectValue = validationDf.getInt("expect_value")

      val vdOperation = validationDf.getString("operation")
      val vdSqlCalc = validationDf.getString("sql_calc")

      val vdSqlCalcApplyString = validationDf.getString("sql_calc_apply")
      val vdSqlCalcApply: List[String] = parseListString(vdSqlCalcApplyString)

      val vdInsertMode = loadType

      val vdMarginPct = validationDf.getInt("margin_pct")

      val vdValidateSeq = validationDf.getInt("validate_seq")

      // The original code had a duplicate assignment `validate_seq = validation_row['sql_calc_apply']`,
      // which appears to be a bug. I've removed it.

      println(s"Validating rule nm : $vdRuleName seq : $vdValidateSeq")
      println(s"vd_schema_name : $vdSchemaName")
      println(s"vd_tbl_name : $vdTableName and tmp_validation is $tmpVdTableName")
      println(s"vd_rule_calc : $vdRuleCalc")
      println(s"vd_rule_calc_apply_col : $vdRuleCalcApplyCol")
      println(s"vd_expect_value : $vdExpectValue")
      println(s"vd_operation : $vdOperation")
      println(s"vd_sql_calc : $vdSqlCalc")
      println(s"vd_sql_calc_apply : $vdSqlCalcApply")
      println(s"vd_insert_mode : $vdInsertMode")
      println(s"vd_margin_pct_acceptable : $vdMarginPct")
      println(s"vd_validate_seq : $vdValidateSeq")
      println("---------------------")

      val isValid = validateTblOperation(
        vdJobName, vdSchemaName, tmpVdTableName, vdRuleCalc, vdRuleCalcApplyCol,
        vdExpectValue, vdOperation, vdSqlCalc, vdSqlCalcApply, vdMarginPct,spark,
        whereCondition
      )

      resultDict.update(s"${vdRuleName}_rule seq : $vdValidateSeq", isValid)
      validationCountSeq += 1
    }

    val keysWithN = resultDict.filter(!_._2).keys.toList
    println(s"Keys with False result: $keysWithN")
    if(keysWithN.nonEmpty) {
      throw new InvalidArgumentException("validate not data not success")
    }
    resultDict.toMap
  }

  def withColumnIncaseOfMissingColumn(tmpzTable: DataFrame, targetTableName: String,
                                      sparkSession: SparkSession,jobNm: String): DataFrame = {
    val targetTableDf = sparkSession.table(targetTableName)
    val executionId = System.currentTimeMillis()
    if(targetTableDf.schema.length > tmpzTable.schema.length) {
      var schemaFoundCount = 0
      targetTableDf.schema.foreach(f => {
        if(autoGenerateColName.contains(f.name)) {
          schemaFoundCount += 1
        }
      })
      if(schemaFoundCount.equals(autoGenerateColName.size)) {
        val tempTable = tmpzTable.select(col("*"),
          lit(executionId).cast(IntegerType).as("execution_id"),
          lit(jobNm),current_timestamp().as("dw_last_update_time"))
        return tempTable.select(targetTableDf.schema.fieldNames.map(tmpzTable(_)): _*)
      }
      return tmpzTable.select(targetTableDf.schema.fieldNames.map(tmpzTable(_)): _*)
    }
    tmpzTable.select(targetTableDf.schema.fieldNames.map(tmpzTable(_)): _*)
  }

  def validateTblOperation(jobName: String, schemaName: String,
                           tblName: String, ruleCalc: String, ruleCalcApplyCol: List[String],
                           expectValue: Double, operation: String, sqlCalc: String = "",
                           sqlCalcApply: List[String] = List.empty, marginPct: Double = 0,
                           spark: SparkSession,whereCondition: String): Boolean = {


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
      spark.sql(s"SELECT $formattedSqlCalc FROM $schemaTempTbl $whereCondition")
    } else {
      spark.sql(s"SELECT ${ruleCalcApplyCol.map(c => s"`$c`").mkString(", ")} FROM $schemaTempTbl $whereCondition")
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
                         updateColumn: List[String] = List.empty,
                         whereCondition: String,path: String,
                         dropPartitionList: List[String]): Unit = {
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
                        updateColumn: List[String],spark: SparkSession,
                        dropPartitionList: List[String]): Unit = {
      val deltaTable = DeltaTable.forName(spark, targetTblName)
      var uniqueKeyList : List[String] = List.empty
      if(uniqueKey != null) {
        uniqueKeyList = scalaObjectMapper.readValue(
          uniqueKey.replace("'", "\""), classOf[List[String]])
      }

      val mergeCond = mergeCondition.orNull
      val joinCondition = uniqueKeyList.map(key => s"trg.$key = src.$key").mkString(" AND ")
      val fullMergeCondition = if(mergeCond != null) {
        var tempCondition = s"$joinCondition AND $mergeCond"
        if(dropPartitionList.nonEmpty) {
          tempCondition = tempCondition + s" AND (${generateWhereConditionFromDeletePartition(dropPartitionList,"trg")})"
        }
        tempCondition
      }
      else {
        var tempCondition = s"$joinCondition"
        if(dropPartitionList.nonEmpty) {
          tempCondition = tempCondition + s" AND (${generateWhereConditionFromDeletePartition(dropPartitionList,"trg")})"
        }
        tempCondition
      }
      logger.info("join condition = {}",joinCondition)
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
      logger.info("merge condition = {}",fullMergeCondition)
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

    val tmpTableNm = s"${tblName}_$tmpValidationNm"

    val schemaTargetTbl = TableUtils.tableSchemaCheck(tblName, schemaName)
    val schemaTempTbl = TableUtils.tableSchemaCheck(tmpTableNm, tmpz)
    var tempTableDf: DataFrame = spark.sql(s"select * from ${schemaTempTbl} ${whereCondition}")
    val tableType = checkTableType(schemaTargetTbl)
    tempTableDf = withColumnIncaseOfMissingColumn(tempTableDf,schemaTargetTbl,spark,jobNm)
    insertMode.toLowerCase match {
      case "full_load" | "overwrite" if (insertMode.toLowerCase() == "full_load") || (partitionColumns.isEmpty) =>
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        val writer = tempTableDf.write.mode("overwrite")
        if (tableType.toLowerCase == "delta") writer.format("delta").insertInto(schemaTargetTbl)
        else writer.format("parquet").insertInto(schemaTargetTbl)

      case "overwrite" if partitionColumns.nonEmpty =>
        println("Overwrite with partition")
        if (tableType.toLowerCase == "delta") {
          spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
          val writer = tempTableDf.write.mode("overwrite")
          //          spark.sql(f"delete from ${schemaTargetTbl} where ${generateWhereConditionFromDeletePartition(dropPartitionList)}")
          writer.format("delta").insertInto(schemaTargetTbl)
        }
        else {
          spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
          val writer = tempTableDf.write.mode("append")
          val path = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tblName,Some(schemaName))).location.toString
          deletePartition(path,schemaTargetTbl,spark,dropPartitionList)
          writer.insertInto(schemaTargetTbl)
        }

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
          spark,
          dropPartitionList
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
