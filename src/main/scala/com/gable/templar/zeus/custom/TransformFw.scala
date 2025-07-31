package com.gable.templar.zeus.custom

import com.gable.templar.constant.JobConstant.JOB_TYPE
import com.gable.templar.custom.view.DependencyCheckModel
import com.gable.templar.heaven.exception.InvalidArgumentException
import com.gable.templar.zeus.SparkServer
import com.gable.templar.zeus.controller.model.LoginUser
import com.gable.templar.zeus.service.vector.ConnectionInfo
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.springframework.web.client.HttpServerErrorException.InternalServerError

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, Temporal, TemporalField}
import scala.collection.JavaConversions._
import java.util.{HashSet, Map, Set}
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class TransformFw extends CustomFw {

  val salt = "rTYlPkZH37QOf7Xx1GzZ0hakdl/2/Z02HlPesDfQ2lM="

  val ultKey = "AdKX67Zn0JRJSGJQ7/4LrQOsZ0IW8+Fcdh7hpeJV8GeVNiPIs4i0RZ4T+XjXyEb0"

  override def doRunFramework(dependencyCheckModel: DependencyCheckModel,
                              JOB_TYPE: JOB_TYPE,sparkSession: SparkSession,loginUser: LoginUser): Unit = {
    val results: java.util.Map[String, Boolean] = checkDependencyByJobName(dependencyCheckModel,sparkSession)
    val notReadyJob: java.util.Set[String] = new java.util.HashSet[String]
    val readyJob: java.util.Set[String] = new java.util.HashSet[String]
    var hasNotReadyJob: Boolean = false
    for (result <- results.entrySet) {
      if (!result.getValue) {
        notReadyJob.add(result.getKey)
        hasNotReadyJob = true
      }
      readyJob.add(result.getKey)
    }
    if (hasNotReadyJob)
      throw new InvalidArgumentException("The dependency job check failed because this job = " + String.join(",", notReadyJob) + " is not finished")
  }

  def checkDependencyByJobName(dependencyCheckModel: DependencyCheckModel,sparkSession: SparkSession): java.util.Map[String,Boolean] = {
    val controlJobDf = sparkSession.sql(f"select back_day, frequency, last_success_ictrl_dt, catchup_type, overlap, ictrl_dt_tgtfmt,specific_argument " +
      f"from ${schemaName}.tbl_job_trans where job_nm = '${dependencyCheckModel.getJobName}'").collect()
    val ictrlDtPattern = controlJobDf(0)(5)
    val frequency = controlJobDf(0)(1)
    val frequencyValue = controlJobDf(0)(6)
    val backdate = controlJobDf(0)(0)
    var masterRefDate: LocalDateTime = null
    if(dependencyCheckModel.getFixedDate == null || dependencyCheckModel.getFixedDate.isEmpty) {
      masterRefDate = minusDateByFrequency(LocalDateTime.now(),frequency.toString,backdate)
    }
    else {
      masterRefDate = LocalDateTime.parse(dependencyCheckModel.getFixedDate,DateTimeFormatter.ofPattern("yyyyMMdd"))
    }
    val currentDate = controlJobDf(0)(2).toString
    val df = sparkSession.sql(s"select * from ${schemaName}.tbl_job_dependency where " +
      s"job_nm = '${dependencyCheckModel.getJobName}' and UPPER(active_flag) = 'Y'")
    val queryMasterSql = s"SELECT system,key,values FROM ${schemaName}.tbl_master_config where system = 'fw_postgre'"
    val connectionInfo = ConnectionService.getMasterConfigLog(queryMasterSql,salt,ultKey)
    val rows = df.collect()
    val returnJobMap = new java.util.HashMap[String,Boolean]()
    rows.foreach(r => {
      var preReqSchemaNm = r.getAs[String]("prerequisite_schema_nm")
      if(!schemaName.endsWith("_uat")) {
        preReqSchemaNm = preReqSchemaNm.replace("_uat","")
      }
      var seqList = checkLog(r.getAs[String]("prerequisite_job_nm"),preReqSchemaNm,
        r.getAs[String]("prerequisite_table_nm"),r.getAs[String]("frequency_check"),
        r.getAs[Any]("value"),r.getAs[Any]("empty_flag"),currentDate,"tbl_trans_audit_logs",
        r.getAs[String]("data_column"),convertPythonDateFormatToJava(
          r.getAs[String]("ictrl_dt_tgtfmt")),masterRefDate,
        frequencyValue.toString,convertPythonDateFormatToJava(ictrlDtPattern.toString),
        currentDate,backdate,connectionInfo,sparkSession)
      if(!seqList._1) {
        seqList = checkLog(r.getAs[String]("prerequisite_job_nm"),preReqSchemaNm,
          r.getAs[String]("prerequisite_table_nm"),r.getAs[String]("frequency_check"),
          r.getAs[Any]("value"),r.getAs[Any]("empty_flag"),currentDate,"tbl_ingest_audit_logs",
          r.getAs[String]("data_column"),convertPythonDateFormatToJava(
            r.getAs[String]("ictrl_dt_tgtfmt")),masterRefDate,
          frequencyValue.toString,convertPythonDateFormatToJava(ictrlDtPattern.toString),
          currentDate,backdate,connectionInfo,sparkSession)
      }
      returnJobMap.put(r.getAs[String]("prerequisite_job_nm"),seqList._1)
    })
    returnJobMap
  }

  def checkLog(
                prerequisiteJobNm: String,
                prerequisiteSchema: String,
                prerequisiteTable: String,
                frequencyCheck: String,
                values: Any, // Can be Int or String depending on usage
                emptyFlag: Any, // Can be Int or String (e.g., "0")
                refDate: String, // String representation of ictrl_dt
                logTable: String,
                businessColumn: String,
                patternIctrlDateCheck: String,
                masterRefDate: LocalDateTime, // LocalDateTime object
                frequency: String,
                patternIctrlDt: String,
                startIctrlDt: String,
                backDate: Any,
                connectionInfo: ConnectionInfo,
                sparkSession: SparkSession
              ): (Boolean, Map[String, Seq[String]]) = {

    println(s"Business column : $businessColumn")
    val tblIngestAuditLogs = s"${schemaName}.tbl_ingest_audit_logs"
    val tblTauditLogs = s"${schemaName}.tbl_trans_audit_logs"
    println(s"tbl_taudit_logs : $tblTauditLogs")

    val queryDict = Map(
      "tbl_ingest_audit_logs" -> s"SELECT target_table_nm, job_start_time, ictrl_dt, status, row_cnt FROM $tblIngestAuditLogs WHERE upper(job_nm) = upper('{{prerequisite_job_nm}}') AND target_schema_nm = '{{prerequisite_schema}}' AND target_table_nm = '{{prerequisite_table}}' AND ictrl_dt = {{target_date}} ORDER BY job_start_time DESC",
      "tbl_trans_audit_logs" -> s"SELECT table_nm, job_start_time, ictrl_dt, status, row_cnt FROM $tblTauditLogs WHERE upper(job_nm) = upper('{{prerequisite_job_nm}}') AND schema_nm = '{{prerequisite_schema}}' AND table_nm = '{{prerequisite_table}}' AND ictrl_dt = {{target_date}} ORDER BY job_start_time DESC"
    )

    var baseQuery: String = queryDict.getOrElse(logTable, throw new InvalidArgumentException(s"Unknown log table type: $logTable"))

    // Query for multi-day frequencies will use a different template
    val queryDictMultiDay = Map(
      "tbl_ingest_audit_logs" -> s"SELECT target_table_nm, job_start_time, ictrl_dt, status, row_cnt, pattern_ictrl_dt FROM $tblIngestAuditLogs WHERE upper(job_nm) = upper('{{prerequisite_job_nm}}') AND target_schema_nm = '{{prerequisite_schema}}' AND target_table_nm = '{{prerequisite_table}}' AND ictrl_dt {{target_date}} ORDER BY job_start_time DESC",
      "tbl_trans_audit_logs" -> s"SELECT table_nm, job_start_time, ictrl_dt, status, row_cnt, pattern_ictrl_dt FROM $tblTauditLogs WHERE upper(job_nm) = upper('{{prerequisite_job_nm}}') AND schema_nm = '{{prerequisite_schema}}' AND table_nm = '{{prerequisite_table}}' AND ictrl_dt {{target_date}} ORDER BY job_start_time DESC"
    )

    val checkInDate = if (businessColumn == null) "logs" else "raw_date"
    println(s"Checking Type : $checkInDate")

    var targetDateQueryPart: String = ""
    var listDateTarget: Seq[String] = Seq.empty[String] // Dates expected to be present
    var records: Seq[Row] = Seq.empty[Row]

    val emptyFlagInt = Try(emptyFlag.toString.toInt).getOrElse(0)
    val valuesInt = Try(values.toString.toInt).getOrElse(0) // Safe conversion

    frequencyCheck match {
      case "daily" | "weekly" | "cur_month" | "prev_month" | "eom" | "day-n" | "hourly" =>
        var targetDateAsDateTime: LocalDateTime = masterRefDate

        frequencyCheck match {
          case "weekly" =>
            // Value is on day of week Sunday = 0, Monday = 1, ..., Saturday = 6
            val dayOfWeek = masterRefDate.getDayOfWeek.getValue % 7 // Monday = 1 (1-7), so %7 makes Sunday = 0, Mon = 1 ... Sat = 6
            val dayReturn = (dayOfWeek + (7 - valuesInt)) % 7 // Ensure positive days to subtract to reach target weekday
            targetDateAsDateTime = masterRefDate.minusDays(dayReturn)
          case "cur_month" =>
            targetDateAsDateTime = masterRefDate.withDayOfMonth(valuesInt)
          case "prev_month" =>
            targetDateAsDateTime = masterRefDate.minusMonths(1).withDayOfMonth(valuesInt)
          case "eom" =>
            targetDateAsDateTime = masterRefDate.withDayOfMonth(1).minusDays(1) // Last day of previous month
          case "day-n" =>
            targetDateAsDateTime = masterRefDate.minusDays(valuesInt)
          case "hourly" =>
            targetDateAsDateTime = masterRefDate.minusHours(valuesInt)
          case _ => // "daily" case
            targetDateAsDateTime = masterRefDate
        }
        targetDateQueryPart = s"'${targetDateAsDateTime.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}'"
        listDateTarget = Seq(targetDateAsDateTime.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck)))

        val msg = s"ref_date = ${targetDateQueryPart}\n" +
          s"query = ${baseQuery.replace("{{prerequisite_job_nm}}", prerequisiteJobNm)
            .replace("{{prerequisite_schema}}", prerequisiteSchema)
            .replace("{{prerequisite_table}}", prerequisiteTable)
            .replace("{{target_date}}", targetDateQueryPart)}\n"
        println(msg)

        if (checkInDate == "logs") {
          val queryRecordsStr = baseQuery.replace("{{prerequisite_job_nm}}", prerequisiteJobNm)
            .replace("{{prerequisite_schema}}", prerequisiteSchema)
            .replace("{{prerequisite_table}}", prerequisiteTable)
            .replace("{{target_date}}", targetDateQueryPart)
          records = postgresqlQueryFunc(connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName, connectionInfo.getUserNm, connectionInfo.getPassword, queryRecordsStr,sparkSession).collect()
        } else if (checkInDate == "raw_date") {
          if (businessColumn != null) {
            println("check in date is raw data")
            records = checkBusinessColumn(frequencyCheck, businessColumn, prerequisiteSchema, prerequisiteTable, targetDateAsDateTime.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck)),sparkSession)
          } else {
            records = Seq.empty[Row]
          }
        }

        if (records.isEmpty) {
          val msg = "table is no record.\n"
          println(msg)
          (false, Map(prerequisiteJobNm -> listDateTarget))
        } else if (businessColumn != null) {
          if (records.nonEmpty) {
            val msg = "table is record.\n"
            println(msg)
            (true, Map(prerequisiteJobNm -> Seq.empty[String])) // Check Pass
          } else { // Should not be reached if records.nonEmpty
            (false, Map(prerequisiteJobNm -> listDateTarget))
          }
        } else {
          val status = records.head.getString(3) // Assuming index 3 for status
          val rowCnt = Option(records.head.get(4)).map(_.toString.toLong).getOrElse(0L) // Assuming index 4 for row_cnt

          val msg = s"status = $status\n" + s"row_cnt = $rowCnt\n"
          println(s"empty_flag : $emptyFlag")
          println(s"row_cnt : $rowCnt")

          if (status == "SUCCEED" && rowCnt >= emptyFlagInt) {
            println("table is ready.\n")
            (true, Map(prerequisiteJobNm -> Seq.empty[String])) // Check Pass
          } else {
            println("table is not ready.\n")
            (false, Map(prerequisiteJobNm -> listDateTarget))
          }
        }

      case "quarter" | "month_to_date" | "hour_to_date" | "daily_period" | "year_to_date" =>
        baseQuery = queryDictMultiDay.getOrElse(logTable, throw new InvalidArgumentException(s"Unknown log table type: $logTable"))

        frequencyCheck match {
          case "quarter" =>
            val monthStartList = List(1, 4, 7, 10)
            if (!(1 <= valuesInt && valuesInt <= 4)) {
              println(s"values not in 1-4 quarter: $values")
              throw new InvalidArgumentException(s"values not in 1-4 quarter: $values")
            }
            val quarterStartMonth = monthStartList(valuesInt - 1)
            println(s"Target Quarter : $valuesInt Month Quarter Start : $quarterStartMonth")

            val dateRun = masterRefDate
            val monthRun = dateRun.getMonthValue

            val currentQuarter = (monthRun - 1) / 3 + 1

            var quarterStartDate: LocalDateTime = null
            var quarterEndDate: LocalDateTime = null

            if (valuesInt > currentQuarter) {
              quarterStartDate = dateRun.minusYears(1).withMonth(quarterStartMonth).withDayOfMonth(1)
              quarterEndDate = quarterStartDate.plusMonths(3).minusDays(1)
            } else if (valuesInt < currentQuarter) {
              quarterStartDate = dateRun.withMonth(quarterStartMonth).withDayOfMonth(1)
              quarterEndDate = quarterStartDate.plusMonths(3).minusDays(1)
            } else { // valuesInt == currentQuarter
              quarterStartDate = dateRun.withMonth(quarterStartMonth).withDayOfMonth(1)
              quarterEndDate = dateRun
            }

            // Generate list of target dates
            var tempDate = quarterStartDate
            while (!tempDate.isAfter(quarterEndDate)) {
              listDateTarget = listDateTarget :+ tempDate.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))
              tempDate = tempDate.plusDays(1)
            }
            listDateTarget = listDateTarget.distinct // Drop duplicates

            val useBetweenQuery = checkOrderDatetimeFormat(patternIctrlDateCheck)
            if (useBetweenQuery) {
              targetDateQueryPart = s"BETWEEN '${quarterStartDate.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}' AND '${quarterEndDate.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}'"
            } else {
              targetDateQueryPart = s"IN (${listDateTarget.map(d => s"'$d'").mkString(", ")})"
            }

          case "month_to_date" =>
            val dateRun = masterRefDate
            val dateStart = dateRun.withDayOfMonth(1)
            var dateEnd = dateRun.withDayOfMonth(valuesInt)

            if (frequency == "monthly" && Try(backDate.toString.toInt).getOrElse(0) == 0) {
              dateEnd = dateEnd.minusDays(1)
            }

            var tempDate = dateStart
            while (!tempDate.isAfter(dateEnd)) {
              listDateTarget = listDateTarget :+ tempDate.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))
              tempDate = tempDate.plusDays(1)
            }
            listDateTarget = listDateTarget.distinct

            val useBetweenQuery = checkOrderDatetimeFormat(patternIctrlDateCheck)
            if (useBetweenQuery) {
              targetDateQueryPart = s"BETWEEN '${dateStart.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}' AND '${dateEnd.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}'"
            } else {
              targetDateQueryPart = s"IN (${listDateTarget.map(d => s"'$d'").mkString(", ")})"
            }

          case "year_to_date" =>
            val dateRun = masterRefDate
            val dateStart = dateRun.withMonth(1).withDayOfMonth(1)
            val dateEnd = dateStart.plusDays(valuesInt - 1) // Add days to reach the target day of year

            var tempDate = dateStart
            while (!tempDate.isAfter(dateEnd)) {
              listDateTarget = listDateTarget :+ tempDate.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))
              tempDate = tempDate.plusDays(1)
            }
            listDateTarget = listDateTarget.distinct

            val useBetweenQuery = checkOrderDatetimeFormat(patternIctrlDateCheck)
            if (useBetweenQuery) {
              targetDateQueryPart = s"BETWEEN '${dateStart.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}' AND '${dateEnd.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}'"
            } else {
              targetDateQueryPart = s"IN (${listDateTarget.map(d => s"'$d'").mkString(", ")})"
            }

          case "hour_to_date" =>
            val dateRun = masterRefDate
            val dateStart = dateRun.withHour(0).withMinute(0).withSecond(0)
            val dateEnd = dateRun.withHour(valuesInt) // Set hour to valuesInt

            var tempHour = dateStart
            while (!tempHour.isAfter(dateEnd)) {
              listDateTarget = listDateTarget :+ tempHour.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))
              tempHour = tempHour.plusHours(1)
            }
            listDateTarget = listDateTarget.distinct

            val useBetweenQuery = checkOrderDatetimeFormat(patternIctrlDateCheck)
            if (useBetweenQuery) {
              targetDateQueryPart = s"BETWEEN '${dateStart.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}' AND '${dateEnd.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}'"
            } else {
              targetDateQueryPart = s"IN (${listDateTarget.map(d => s"'$d'").mkString(", ")})"
            }

          case "daily_period" =>
            // refDate and startIctrlDt are strings, so parse them
            val formatter = DateTimeFormatter.ofPattern(patternIctrlDt)
            val dateRunLocal = LocalDateTime.parse(refDate, formatter)
            val dateStartLocal = LocalDateTime.parse(startIctrlDt, formatter)
            val dateEndLocal = dateRunLocal

            var currentDate = dateStartLocal
            while (!currentDate.isAfter(dateEndLocal)) {
              listDateTarget = listDateTarget :+ currentDate.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))
              currentDate = addDateByFrequency(currentDate, frequency)
            }
            listDateTarget = listDateTarget.distinct

            val useBetweenQuery = checkOrderDatetimeFormat(patternIctrlDateCheck)
            if (useBetweenQuery) {
              targetDateQueryPart = s"BETWEEN '${dateStartLocal.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}' AND '${dateEndLocal.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}'"
            } else {
              targetDateQueryPart = s"IN (${listDateTarget.map(d => s"'$d'").mkString(", ")})"
            }
        } // End of inner frequencyCheck match

        val formattedQuery = baseQuery.replace("{{prerequisite_job_nm}}", prerequisiteJobNm)
          .replace("{{prerequisite_schema}}", prerequisiteSchema)
          .replace("{{prerequisite_table}}", prerequisiteTable)
          .replace("{{target_date}}", targetDateQueryPart) // This part contains BETWEEN or IN clause
        println(s"query = $formattedQuery\n")

        if (checkInDate.toLowerCase == "logs") {
          records = postgresqlQueryFunc(connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName, connectionInfo.getUserNm, connectionInfo.getPassword, formattedQuery,sparkSession).collect()
        } else if (checkInDate.toLowerCase == "raw_date") {
          records = checkBusinessColumn(frequencyCheck, businessColumn, prerequisiteSchema, prerequisiteTable, targetDateQueryPart,sparkSession) // Pass the formatted string directly
        }

        if (records.isEmpty) {
          println("table is no record.\n")
          (false, Map(prerequisiteJobNm -> listDateTarget))
        } else if (businessColumn != null) {
          val listLogDate = records.map(r => r.getString(0)) // Assuming business column is first in select
          val findMiss = listDateTarget.toSet -- listLogDate.toSet
          val msg = s"Find in not list : $findMiss\n"

          if (findMiss.isEmpty) {
            println("table is ready.\n")
            (true, Map(prerequisiteJobNm -> Seq.empty[String]))
          } else {
            println("table is not ready.\n")
            (false, Map(prerequisiteJobNm -> findMiss.toSeq.sorted))
          }
        } else {
          val listLogDate = records.filter(r => r.getString(3) == "SUCCEED" && Option(r.get(4)).map(_.toString.toLong).getOrElse(0L) >= emptyFlagInt)
            .map(r => r.getString(2)) // Assuming index 2 is ictrl_date

          val findMiss = listDateTarget.toSet -- listLogDate.toSet
          val msg = s"Find in not list : $findMiss\n"

          if (findMiss.isEmpty) {
            println("table is ready.\n")
            (true, Map(prerequisiteJobNm -> Seq.empty[String]))
          } else {
            println("table is not ready.\n")
            (false, Map(prerequisiteJobNm -> findMiss.toSeq.sorted))
          }
        }

      case "max_date" =>
        baseQuery = queryDictMultiDay.getOrElse(logTable, throw new InvalidArgumentException(s"Unknown log table type: $logTable"))

        val dateQueryStr = masterRefDate.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))

        val useBetweenQuery = checkOrderDatetimeFormat(patternIctrlDateCheck)
        if (useBetweenQuery) {
          targetDateQueryPart = s"AND ictrl_dt >= '$dateQueryStr'" // For log tables, we select based on ictrl_dt
        } else {
          targetDateQueryPart = "" // Get all logs if format doesn't support BETWEEN
        }

        val formattedQuery = baseQuery.replace("{{prerequisite_job_nm}}", prerequisiteJobNm)
          .replace("{{prerequisite_schema}}", prerequisiteSchema)
          .replace("{{prerequisite_table}}", prerequisiteTable)
          .replace("{{target_date}}", targetDateQueryPart)
        println(s"query = $formattedQuery\n")

        if (checkInDate.toLowerCase == "logs") {
          records = postgresqlQueryFunc(connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName, connectionInfo.getUserNm, connectionInfo.getPassword, formattedQuery,sparkSession).collect()
        } else if (checkInDate.toLowerCase == "raw_date") {
          var effectiveFrequencyCheck = frequencyCheck
          var effectiveTargetDateForBusinessCol = dateQueryStr
          if (useBetweenQuery) {
            effectiveFrequencyCheck = "max_date_between" // Adjust frequency for check_business_column
            effectiveTargetDateForBusinessCol = dateQueryStr
          } else {
            // If not using BETWEEN, check_business_column might need a different strategy
            // The original Python code for 'max_date' on 'raw_date' just uses the 'max_day' query
            // which doesn't have a WHERE clause, so `target_date` isn't used there.
            // We'll mimic that by passing an empty string if `useBetweenQuery` is false and it's `raw_date`
            effectiveFrequencyCheck = "max_date"
            effectiveTargetDateForBusinessCol = ""
          }
          records = checkBusinessColumn(effectiveFrequencyCheck, businessColumn, prerequisiteSchema, prerequisiteTable, effectiveTargetDateForBusinessCol,sparkSession)
        }

        if (records.isEmpty) {
          println("table is no record.\n")
          (false, Map(prerequisiteJobNm -> Seq(dateQueryStr)))
        } else if (businessColumn != null) {
          val listLogDate = records.map(r => LocalDateTime.parse(r.getString(0), DateTimeFormatter.ofPattern(patternIctrlDateCheck)))
          val lastDatetime = listLogDate.maxBy(_.toEpochSecond(java.time.ZoneOffset.UTC)) // Get max date

          println(s"Last Datetime : $lastDatetime")

          if (lastDatetime.isEqual(masterRefDate) || lastDatetime.isAfter(masterRefDate)) {
            println("table is ready.\n")
            (true, Map(prerequisiteJobNm -> Seq.empty[String]))
          } else {
            println("table is not ready.\n")
            (false, Map(prerequisiteJobNm -> Seq(dateQueryStr)))
          }
        } else {
          val successfulRecords = records.filter(r => r.getString(3) == "SUCCEED" && Option(r.get(4)).map(_.toString.toLong).getOrElse(0L) >= emptyFlagInt)
          if (successfulRecords.isEmpty) {
            println("table is not ready (no successful records).\n")
            (false, Map(prerequisiteJobNm -> Seq(dateQueryStr)))
          } else {
            val listLogDate = successfulRecords.map(r => LocalDateTime.parse(r.getString(2), DateTimeFormatter.ofPattern(patternIctrlDateCheck)))
            val lastDatetime = listLogDate.maxBy(_.toEpochSecond(java.time.ZoneOffset.UTC))

            println(s"Last Datetime : $lastDatetime")

            if (lastDatetime.isEqual(masterRefDate) || lastDatetime.isAfter(masterRefDate)) {
              println("table is ready.\n")
              (true, Map(prerequisiteJobNm -> Seq.empty[String]))
            } else {
              println("table is not ready.\n")
              (false, Map(prerequisiteJobNm -> Seq(dateQueryStr)))
            }
          }
        }

      case "date_range" =>
        baseQuery = queryDictMultiDay.getOrElse(logTable, throw new InvalidArgumentException(s"Unknown log table type: $logTable"))

        val dateRun = masterRefDate
        val multiEndDate = masterRefDate
        val multiStartDate = masterRefDate.minusDays(valuesInt)

        var tempDate = multiStartDate
        while (!tempDate.isAfter(multiEndDate)) {
          listDateTarget = listDateTarget :+ tempDate.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))
          tempDate = tempDate.plusDays(1)
        }
        listDateTarget = listDateTarget.distinct

        val useBetweenQuery = checkOrderDatetimeFormat(patternIctrlDateCheck)
        if (useBetweenQuery) {
          targetDateQueryPart = s"BETWEEN '${multiStartDate.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}' AND '${multiEndDate.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}'"
        } else {
          targetDateQueryPart = s"IN (${listDateTarget.map(d => s"'$d'").mkString(", ")})"
        }

        val formattedQuery = baseQuery.replace("{{prerequisite_job_nm}}", prerequisiteJobNm)
          .replace("{{prerequisite_schema}}", prerequisiteSchema)
          .replace("{{prerequisite_table}}", prerequisiteTable)
          .replace("{{target_date}}", targetDateQueryPart)
        println(s"query = $formattedQuery\n")

        if (checkInDate.toLowerCase == "logs") {
          records = postgresqlQueryFunc(connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName, connectionInfo.getUserNm, connectionInfo.getPassword, formattedQuery,sparkSession).collect()
        } else if (checkInDate.toLowerCase == "raw_date") {
          records = checkBusinessColumn(frequencyCheck, businessColumn, prerequisiteSchema, prerequisiteTable, targetDateQueryPart,sparkSession)
        }

        if (records.isEmpty) {
          println("table is no record.\n")
          (false, Map(prerequisiteJobNm -> listDateTarget))
        } else if (businessColumn != null) {
          val listLogDate = records.map(r => r.getString(0))
          val findMiss = listDateTarget.toSet -- listLogDate.toSet
          val msg = s"Find in not list : $findMiss\n"

          if (findMiss.isEmpty) {
            println("table is ready.\n")
            (true, Map(prerequisiteJobNm -> Seq.empty[String]))
          } else {
            println("table is not ready.\n")
            (false, Map(prerequisiteJobNm -> findMiss.toSeq.sorted))
          }
        } else {
          val listLogDate = records.filter(r => r.getString(3) == "SUCCEED" && Option(r.get(4)).map(_.toString.toLong).getOrElse(0L) >= emptyFlagInt)
            .map(r => r.getString(2))

          val findMiss = listDateTarget.toSet -- listLogDate.toSet
          val msg = s"Find in not list : $findMiss\n"

          if (findMiss.isEmpty) {
            println("table is ready.\n")
            (true, Map(prerequisiteJobNm -> Seq.empty[String]))
          } else {
            println("table is not ready.\n")
            (false, Map(prerequisiteJobNm -> findMiss.toSeq.sorted))
          }
        }

      case "date_in_range" =>
        baseQuery = queryDictMultiDay.getOrElse(logTable, throw new InvalidArgumentException(s"Unknown log table type: $logTable"))

        val dateRun = masterRefDate
        val valuesInt = Try(values.toString.toInt).getOrElse(0)

        // Calculate theoretical start and end dates for the "BETWEEN" query if applicable
        // The individual dates for the IN clause are generated below.
        val dateStartForBetween = minusDateByFrequency(dateRun, frequency, valuesInt)
        val dateEndForBetween = addDateByFrequency(dateRun, frequency, valuesInt)


        // Generate the list of target dates within the range for comparison
        listDateTarget = Seq.empty[String]

        // Dates on the "minus" side
        for (number <- 1 to valuesInt) {
          val tempDay = minusDateByFrequency(dateRun, frequency, number)
          listDateTarget = listDateTarget :+ tempDay.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))
        }
        // Dates on the "plus" side
        for (number <- 1 to valuesInt) {
          val tempDay = addDateByFrequency(dateRun, frequency, number)
          listDateTarget = listDateTarget :+ tempDay.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))
        }
        // Add the current date
        listDateTarget = listDateTarget :+ dateRun.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))
        listDateTarget = listDateTarget.distinct.sorted // Ensure uniqueness and order

        val useBetweenQuery = checkOrderDatetimeFormat(patternIctrlDateCheck)
        if (useBetweenQuery) {
          targetDateQueryPart = s"BETWEEN '${dateStartForBetween.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}' AND '${dateEndForBetween.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}'"
        } else {
          targetDateQueryPart = s"IN (${listDateTarget.map(d => s"'$d'").mkString(", ")})"
        }

        val formattedQuery = baseQuery.replace("{{prerequisite_job_nm}}", prerequisiteJobNm)
          .replace("{{prerequisite_schema}}", prerequisiteSchema)
          .replace("{{prerequisite_table}}", prerequisiteTable)
          .replace("{{target_date}}", targetDateQueryPart)
        println(s"query = $formattedQuery\n")

        if (checkInDate.toLowerCase == "logs") {
          records = postgresqlQueryFunc(connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName, connectionInfo.getUserNm, connectionInfo.getPassword, formattedQuery,sparkSession).collect()
        } else if (checkInDate.toLowerCase == "raw_date") {
          records = checkBusinessColumn(frequencyCheck, businessColumn, prerequisiteSchema, prerequisiteTable, targetDateQueryPart,sparkSession)
        }

        if (records.isEmpty) {
          println("table is no record.\n")
          (false, Map(prerequisiteJobNm -> listDateTarget))
        } else if (businessColumn != null) {
          val listLogDate = records.map(r => r.getString(0))
          val findMiss = listDateTarget.toSet -- listLogDate.toSet
          val msg = s"Find in not list : $findMiss\n"

          // "Change check if found more at once" implies if there's any overlap, it might be considered true,
          // but the primary check is if *all* target dates are present.
          // The Python `len(find_miss) != len(list_date_target)` condition for true is unusual for a prerequisite check
          // (it means "not all are missing"). A more standard "all required dates are present" is `find_miss.isEmpty`.
          // I will use `find_miss.isEmpty` for a strict "all dates are present" check, which is more robust for prerequisites.
          // If you strictly need the original Python logic (i.e., "at least one record found"),
          // then `records.nonEmpty` is the check.
          // For now, adhering to the `find_miss` logic:
          if (findMiss.isEmpty) { // Assuming "if len(find_miss) == 0" means all are found.
            println("table is ready.\n")
            (true, Map(prerequisiteJobNm -> Seq.empty[String]))
          } else {
            println("table is not ready.\n")
            (false, Map(prerequisiteJobNm -> findMiss.toSeq.sorted))
          }
        } else {
          val listLogDate = records.filter(r => r.getString(3) == "SUCCEED" && Option(r.get(4)).map(_.toString.toLong).getOrElse(0L) >= emptyFlagInt)
            .map(r => r.getString(2))

          val findMiss = listDateTarget.toSet -- listLogDate.toSet
          val msg = s"Find in not list : $findMiss\n"

          if (findMiss.isEmpty) {
            println("table is ready.\n")
            (true, Map(prerequisiteJobNm -> Seq.empty[String]))
          } else {
            println("table is not ready.\n")
            (false, Map(prerequisiteJobNm -> findMiss.toSeq.sorted))
          }
        }

      case _ =>
        val msg = s"$frequencyCheck is not supported.\n"
        println(msg)
        (false, Map(prerequisiteJobNm -> Seq(msg)))
    }
  }

//  def checkLogIngestion(
//                         jobName: String,
//                         schema: String,
//                         table: String,
//                         frequency: String,
//                         emptyFlag: Int,
//                         refDate: LocalDateTime,
//                         businessColumn: Option[String],
//                         pattern: String,
//                         logTable: String,
//                         dbHost: String,
//                         dbPort: String,
//                         dbUser: String,
//                         dbPassword: String,
//                         sid: String,
//                         expectedHours: Int = 1
//                       ): (Boolean, Map[String, Seq[String]]) = {
//
//    val formatter = DateTimeFormatter.ofPattern(pattern)
//    val dateStr = refDate.format(formatter)
//    val query: String = logTable match {
//      case "tbl_ingest_logs" =>
//        s"""
//           |SELECT target_table_nm, job_start_time, ictrl_dt, status, row_cnt
//           |FROM ingestion.tbl_ingest_logs
//           |WHERE upper(job_nm) = upper('$jobName')
//           |  AND target_schema_nm = '$schema'
//           |  AND target_table_nm = '$table'
//           |  AND ictrl_dt LIKE '$dateStr%'
//           |ORDER BY job_start_time DESC
//           |""".stripMargin
//      case "tbl_taudit_logs" =>
//        s"""
//           |SELECT table_nm, job_start_time, ictrl_dt, status, row_cnt
//           |FROM ingestion.tbl_taudit_logs
//           |WHERE upper(job_nm) = upper('$jobName')
//           |  AND schema_nm = '$schema'
//           |  AND table_nm = '$table'
//           |  AND ictrl_dt LIKE '$dateStr%'
//           |ORDER BY job_start_time DESC
//           |""".stripMargin
//      case _ => throw new InvalidArgumentException("Invalid log table provided")
//    }
//
//    val records: Array[Row] = {
//      val df = oracleIngestionQueryLog(dbHost, dbPort, dbUser, dbPassword, query, sid)
//      df.collect()
//    }
//
//    def generateHourlyWindows(date: LocalDateTime, count: Int): Seq[String] = {
//      val formatter = DateTimeFormatter.ofPattern(pattern)
//      (0 until count).map(h => date.minusHours(h.toLong).format(formatter)).reverse
//    }
//
//    def generate24HourWindows(date: LocalDateTime): Seq[String] = {
//      val start = date.withHour(0).withMinute(0).withSecond(0)
//      (0 to 23).map(h => start.plusHours(h).format(formatter))
//    }
//
//    val checkInRaw = businessColumn.exists(_.nonEmpty)
//
//    def extractDates(rows: Array[Row]): Seq[String] = {
//      rows.map(_.getAs[Any]("ictrl_dt").toString).distinct
//    }
//
//    frequency match {
//      case "daily" =>
//        if (records.nonEmpty) {
//          val status = records.head.getAs[String]("status")
//          val rowCnt = Option(records.head.getAs[Object]("row_cnt")).map(_.toString.toInt).getOrElse(0)
//          if (status == "SUCCEED" && rowCnt >= emptyFlag)
//            return (true, Map(jobName -> Seq()))
//        }
//        if (checkInRaw) {
//          val rawRecords = checkBusinessColumn(frequency, businessColumn.get, schema, table, dateStr)
//          if (rawRecords.nonEmpty) (true, Map(jobName -> Seq()))
//          else (false, Map(jobName -> Seq(dateStr)))
//        } else (false, Map(jobName -> Seq(dateStr)))
//
//      case "hourly" =>
//        val hourAgo = refDate.minusHours(expectedHours)
//        val hourlyStr = hourAgo.format(formatter)
//        if (records.exists(_.getAs[Any]("ictrl_dt").toString == hourlyStr)) (true, Map(jobName -> Seq()))
//        else (false, Map(jobName -> Seq(hourlyStr)))
//
//      case "24hours" =>
//        val expectedDates = generate24HourWindows(refDate)
//        val logDates = extractDates(records)
//
//        val missing = expectedDates.diff(logDates)
//        if (missing.isEmpty) (true, Map(jobName -> Seq()))
//        else (false, Map(jobName -> missing))
//
//      case "every_n_hour" =>
//        val totalSlots = 24
//        val required = totalSlots / expectedHours
//        val expectedDates = generate24HourWindows(refDate)
//        val logDates = extractDates(records)
//        val actualCount = expectedDates.count(logDates.contains)
//
//        if (actualCount >= required) (true, Map(jobName -> Seq()))
//        else {
//          val missing = expectedDates.diff(logDates)
//          (false, Map(jobName -> missing))
//        }
//
//      case _ => throw new InvalidArgumentException(s"Unsupported frequency type: $frequency")
//    }
//  }

}
