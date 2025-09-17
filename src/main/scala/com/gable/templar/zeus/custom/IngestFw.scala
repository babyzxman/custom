package com.gable.templar.zeus.custom

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.gable.templar.constant.JobConstant
import com.gable.templar.constant.JobConstant.{CATCHUP_TYPE, LOAD_TYPE}
import com.gable.templar.custom.view.{DependencyCheckModel, ExecuteResponse, RunNotebookParallelResult}
import com.gable.templar.exception.{DropDuplicatesJobError, DropSuccessJobError, RunNotebookParallelException}
import com.gable.templar.heaven.exception.InvalidArgumentException
import com.gable.templar.zeus.controller.model.LoginUser
import com.gable.templar.zeus.service.vector.ConnectionInfo
import org.apache.spark.sql.functions.{col, lower}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory
import org.springframework.core.task.TaskExecutor

import java.sql.{ResultSet, Timestamp}
import scala.collection.JavaConversions._
import java.text.SimpleDateFormat
import java.time.{Duration, LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.concurrent.{Callable, CompletableFuture, Future}
import java.util.function.Supplier
import java.{lang, util}
import javax.servlet.http.HttpServletRequest
import scala.:+
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}
import scala.util.control.Breaks.{break, breakable}
import scala.util.matching.Regex

class IngestFw(override val schemaName: String,
               override val heraUrl: String,
               override val loginUser: LoginUser,
               override val sparkSession: SparkSession,
               override val taskExecutor: TaskExecutor) extends CustomFw {

  private val logger = LoggerFactory.getLogger(classOf[TransformFw])


  override def insertAuditLogDetail(stepRun:String,stepSeq:String,jobName: String,
                                    dagRunId: String, tasksGroupNm: String,schemaName: String,
                                    tableName: String,loadType: String,roundTime:LocalDateTime,
                                    detailStartTime: LocalDateTime,status: String,ictrlDt: String,
                                    stepRunNext:String,stepSeqNext:String,
                                    connectionInfo: ConnectionInfo,auditLogDetail: String,
                                    auditLogDetailNext: String): Unit = {
    val detailEndTime = LocalDateTime.now()
    val duration = Duration.between(detailStartTime, detailEndTime)
    val hours = duration.toHours
    val minutes = duration.minusHours(hours).toMinutes
    val seconds = duration.minusHours(hours).minusMinutes(minutes).getSeconds
    val durationString = f"$hours%02d:$minutes%02d:$seconds%02d"
    val sql = s"""INSERT INTO ${this.schemaName}.$auditLogDetail (job_nm,tasksgroup_nm,round_time,dag_run_id,target_schema_nm,target_table_nm,load_type,job_start_time,job_end_time,duration,ictrl_dt,step_run,step_seq,status,err_msg)
                 |                VALUES ('${jobName}','${tasksGroupNm}','${roundTime}','${dagRunId}'
                 |                ,'${schemaName}','${tableName}','${loadType}','${detailStartTime}','${detailEndTime}',
                 |                '${durationString}','${ictrlDt}','${stepRun}','${stepSeq}','${status}','-')""".stripMargin
    ConnectionService.postgresqlInsertUpdateFunc(
      connectionInfo.getIp,connectionInfo.getPort,connectionInfo.getDbName,
      connectionInfo.getUserNm,connectionInfo.getPassword,sql,Seq.empty)
    if((stepRunNext != null && stepRunNext == "") || (stepSeqNext != null && stepSeqNext == "")) {
      val queryInsertLog = s"""INSERT INTO ${this.schemaName}.$auditLogDetailNext (job_nm,tasksgroup_nm,round_time,job_start_time,ictrl_dt,step_run,step_seq)
                              |                    VALUES ('${jobName}', '${tasksGroupNm}', '${roundTime}', '${detailStartTime}', '${ictrlDt}', '${stepRunNext}','${stepSeqNext}')""".stripMargin
      ConnectionService.postgresqlInsertUpdateFunc(
        connectionInfo.getIp,connectionInfo.getPort,connectionInfo.getDbName,
        connectionInfo.getUserNm,connectionInfo.getPassword,queryInsertLog,Seq.empty)
    }
  }

  case class RowData(
                      frequencyCheck: String,
                      prerequisiteTableNm: String,
                      prerequisiteSchemaNm: String,
                      prerequisiteJobNm: String,
                      value: String,
                      emptyFlag: Int,
                      dataColumn: String,
                      ictrlDtTgtfmt: String
                    )

  val blackListNullString = Set("","null", "none", "nan", "none", "na")


  def processResultSetWithSingleLoop(resultSet: ResultSet): (Boolean, List[String], Option[RowData]) = {

    var firstRow: Option[RowData] = None
    val prerequisiteJobNames = ListBuffer.empty[String]

    var allUnique = true

    breakable {
      while (resultSet.next()) {
        val currentRow = RowData(
          frequencyCheck = resultSet.getString("frequency_check"),
          prerequisiteTableNm = resultSet.getString("prerequisite_table_nm"),
          prerequisiteSchemaNm = resultSet.getString("prerequisite_schema_nm"),
          prerequisiteJobNm = resultSet.getString("prerequisite_job_nm"),
          value = resultSet.getString("value"),
          emptyFlag = resultSet.getInt("empty_flag"),
          dataColumn = resultSet.getString("data_column"),
          ictrlDtTgtfmt = resultSet.getString("ictrl_dt_tgtfmt")
        )

        if (firstRow.isEmpty) {
          firstRow = Some(currentRow)
        } else {
          // Check for uniqueness against the first row's values
          val prevRow = firstRow.get
          if (prevRow.frequencyCheck != currentRow.frequencyCheck ||
            prevRow.prerequisiteTableNm != currentRow.prerequisiteTableNm ||
            prevRow.prerequisiteSchemaNm != currentRow.prerequisiteSchemaNm) {
            allUnique = false
            break()
          }
        }
        prerequisiteJobNames += currentRow.prerequisiteJobNm
      }
    }

    // If the result set was empty
    if (prerequisiteJobNames.isEmpty) {
      (false, List.empty[String], None)
    }
    // Check the final conditions
    else if (allUnique && firstRow.get.frequencyCheck.toLowerCase == "daily") {
      (true, prerequisiteJobNames.toList, firstRow)
    }
    // If conditions are not met
    else {
      (false, List.empty[String], None)
    }
  }


  def postProcess(status: String,
                           dependencyCheckModel: DependencyCheckModel,
                           errorMsg: String,
                           runId: String, sparkSession: SparkSession, logUrl: String,
                           jobStartTime: LocalDateTime, jobEndTime: LocalDateTime,
                           refDate: LocalDateTime, connectionInfo: ConnectionInfo,
                           ictrlDt: String, jobName: String, tblLogName: String,
                           tblConfName: String,lastSuccessIctrlDt: String): Unit = {
    val duration = Duration.between(jobStartTime, jobEndTime)
    val hours = duration.toHours
    val minutes = duration.minusHours(hours).toMinutes
    val seconds = duration.minusHours(hours).minusMinutes(minutes).getSeconds
    val durationString = f"$hours%02d:$minutes%02d:$seconds%02d"
    var params: Seq[Any] = Seq.empty
    var sql: String = null
    params = Seq(status, logUrl,
      durationString, jobEndTime,jobEndTime, jobName, runId, ictrlDt, refDate)
    sql = f"update ${this.schemaName}.$tblLogName set status = ?, " +
      f"log_url = ?, duration = ?, job_end_time = ?, custom_end_time = ?  " +
      f"where job_nm = ? and dag_run_id = ? and ictrl_dt = ? " +
      f"and round_time = ?"
    ConnectionService.postgresqlInsertUpdateFunc(connectionInfo.getIp,
      connectionInfo.getPort, connectionInfo.getDbName,
      connectionInfo.getUserNm, connectionInfo.getPassword, sql, params)
//    if (status.equals("SUCCEED") && (lastSuccessIctrlDt == null || lastSuccessIctrlDt.toInt < ictrlDt.toInt)){
//      val sql = s"update ${this.schemaName}.$tblConfName set last_success_ictrl_dt = '$ictrlDt' " +
//        s"where job_nm = '${jobName}'"
//      ConnectionService.postgresqlInsertUpdateFunc(connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
//        connectionInfo.getUserNm, connectionInfo.getPassword, sql, Seq.empty)
//    }
  }

  def checkFrequencyAndTgtFmt(frequency: String, ictrlDtTgtFmt: String): Boolean = {
    val correctFormat = frequency.toLowerCase match {
      case "daily" | "weekly" => {
        "yyyyMMdd"
      }
      case "monthly" => {
        "yyyyMM"
      }
      case "hourly" => {
        "yyyyMMddHH"
      }
    }
    correctFormat == ictrlDtTgtFmt
  }

  def checkRunningIctrlDtIngest(jobNmUpdate: String, tasksgroupNmUpdate: String,
                                 ictrlDtUpdate: String, roundTime: LocalDateTime, jobStartTimeUpdate: LocalDateTime,
                                 startIctrlDtStrUpdate: String, endIctrlDtStrUpdate: String, processJobType: String,
                                 cdrFlag: Boolean = false, dagRunId: String, appIdUpdate: String,
                                 frequency: String, connectionInfo: ConnectionInfo, ictrlDtTgtFmt: String): Unit = {


    println(s"Running time to check log: ${LocalDateTime.now()}")
    println(s"Round_time On Checking: $roundTime")

    val hoursCheckRoundTime = 3
    val ingestAuditLogsTable = s"$schemaName.tbl_ingest_audit_logs"
    val queryIngLog = if (!cdrFlag) {
      s"""
    SELECT job_nm, tasksgroup_nm, round_time, dag_run_id, target_schema_nm, target_table_nm, job_start_time, job_end_time, ictrl_dt, status
    FROM $ingestAuditLogsTable
    WHERE lower(job_nm) = lower('$jobNmUpdate') AND lower(tasksgroup_nm) =
    lower('$tasksgroupNmUpdate') AND ictrl_dt = '$ictrlDtUpdate' AND (status = 'RUNNING' OR status = 'WAITING')
    AND round_time != '$roundTime'
    ORDER BY round_time DESC
    LIMIT 1
    """
    } else {
      println(s"CDR FLAG IS $cdrFlag")
      s"""
    SELECT job_nm, tasksgroup_nm, round_time, dag_run_id, target_schema_nm, target_table_nm, job_start_time, job_end_time, ictrl_dt, status
    FROM $ingestAuditLogsTable
    WHERE lower(job_nm) = lower('$jobNmUpdate') AND lower(tasksgroup_nm) = lower('$tasksgroupNmUpdate')
    AND ictrl_dt IS NOT NULL AND (status = 'RUNNING' OR status = 'WAITING') AND round_time != '$roundTime'
    ORDER BY round_time DESC
    LIMIT 1
    """
    }
    val dfResultLog = ConnectionService.postgresqlQueryDirectly(
      connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
      connectionInfo.getUserNm, connectionInfo.getPassword, queryIngLog)
    try {
      // Has Log Running
      breakable {
        while (dfResultLog.rs.next()) {
          val lastRoundTimeWIctrlDt = dfResultLog.rs.getTimestamp("round_time")

          val strFormatRoundTime = new java.text.SimpleDateFormat("yyyyMMdd").format(lastRoundTimeWIctrlDt)
          val strRoundTimeIn = roundTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
          if (strFormatRoundTime != strRoundTimeIn) {
            println("Check log Complete, current round_time is newer by 1 day than the last round_time.")
            break()
          } else {
            val errMsg = s"Found Status RUNNING Job on ictrl_dt: $ictrlDtUpdate Running ON -> dag_run_id: ${dfResultLog.rs.getString("dag_run_id")} round_time: ${dfResultLog.rs.getTimestamp("round_time")}"
            val errMsgUpdate = s"DropDuplicatesJobError: $errMsg"
            println("-- Has Log Running in ingest audit log --")
            println("Update Log Function")
            val queryUpdateLog =
              s"""
        UPDATE $ingestAuditLogsTable
        SET job_start_time = '$jobStartTimeUpdate',
        ictrl_dt = '$ictrlDtUpdate',
        start_ictrl_dt = '$startIctrlDtStrUpdate',
        end_ictrl_dt = '$endIctrlDtStrUpdate',
        status = 'FAILED',
        job_end_time = '$jobStartTimeUpdate',
        duration = '00:00:00',
        source_cnt = 0,
        process_cnt = 0,
        row_cnt = 0,
        err_msg = '$errMsgUpdate',
        app_id = '$appIdUpdate'
        WHERE job_nm = '$jobNmUpdate' AND tasksgroup_nm = '$tasksgroupNmUpdate' AND round_time = '$roundTime' AND dag_run_id = '$dagRunId'
        """
            ConnectionService.postgresqlInsertUpdateFunc(
              connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
              connectionInfo.getUserNm, connectionInfo.getPassword, queryUpdateLog, Seq.empty
            )
            println("Update audit log Complete")
            throw new DropDuplicatesJobError(errMsg)
          }
        }
      }
    }
    finally{
      dfResultLog.close()
    }

    // Checking Running Ictrl_dt With Null
    val checkRoundTime = roundTime.minusHours(hoursCheckRoundTime)
    val strCheckRoundTime = checkRoundTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val strCheckCurrectRoundTime = roundTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    val queryIngLogNullIctrlDt =
      s"""
  SELECT job_nm, tasksgroup_nm, round_time, dag_run_id, target_schema_nm, target_table_nm, job_start_time, job_end_time, ictrl_dt, status
  FROM $ingestAuditLogsTable
  WHERE lower(job_nm) = lower('$jobNmUpdate') AND lower(tasksgroup_nm) = lower('$tasksgroupNmUpdate') AND ictrl_dt IS NULL AND (status = 'RUNNING' OR status = 'WAITING') AND round_time >= '$strCheckRoundTime' AND round_time < '$strCheckCurrectRoundTime'
  ORDER BY round_time DESC
  LIMIT 1
  """

    val dfResultLogNull = ConnectionService.postgresqlQueryDirectly(
      connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
      connectionInfo.getUserNm, connectionInfo.getPassword, queryIngLogNullIctrlDt)
    try {
      while (dfResultLogNull.rs.next()) {
        val errMsg = s"Found Status RUNNING round_time under $hoursCheckRoundTime with ictrl_dt NULL : Running ON -> dag_run_id: ${dfResultLogNull.rs.getString("dag_run_id")} round_time: ${dfResultLogNull.rs.getTimestamp("round_time")}"
        val errMsgUpdate = s"DropDuplicatesJobError: $errMsg"
        val queryUpdateLog =
          s"""
      UPDATE $ingestAuditLogsTable
      SET job_start_time = '$jobStartTimeUpdate',
      ictrl_dt = '$ictrlDtUpdate',
      start_ictrl_dt = '$startIctrlDtStrUpdate',
      end_ictrl_dt = '$endIctrlDtStrUpdate',
      status = 'FAILED',
      job_end_time = '$jobStartTimeUpdate',
      duration = '00:00:00',
      source_cnt = 0,
      process_cnt = 0,
      row_cnt = 0,
      err_msg = '$errMsgUpdate',
      app_id = '$appIdUpdate'
      WHERE job_nm = '$jobNmUpdate' AND tasksgroup_nm = '$tasksgroupNmUpdate' AND round_time = '$roundTime' AND dag_run_id = '$dagRunId'
      """
        ConnectionService.postgresqlInsertUpdateFunc(
          connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
          connectionInfo.getUserNm, connectionInfo.getPassword, queryUpdateLog)
        println("Update audit log Complete")
        throw new DropDuplicatesJobError(errMsg)
      }
    }
    finally{
      dfResultLogNull.close()
    }

    // More Codition Check SUCCEED Log
    if (processJobType == "ongoing" && !cdrFlag && checkFrequencyAndTgtFmt(frequency,ictrlDtTgtFmt)) {
      logger.info("test ongoing job logs")
      val queryIngLogSuccess =
        s"""
    SELECT job_nm, tasksgroup_nm, round_time, dag_run_id, target_schema_nm, target_table_nm, job_start_time, job_end_time, ictrl_dt, status
    FROM $ingestAuditLogsTable
    WHERE lower(job_nm) = lower('$jobNmUpdate') AND lower(tasksgroup_nm) = lower('$tasksgroupNmUpdate') AND ictrl_dt = '$ictrlDtUpdate' AND status = 'SUCCEED' AND err_msg = '-'
    ORDER BY round_time DESC
    LIMIT 1
    """
      logger.info("check query ing log success = {}",queryIngLogSuccess)
      val dfResultLogSuccess = ConnectionService.postgresqlQueryDirectly(
        connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
        connectionInfo.getUserNm, connectionInfo.getPassword, queryIngLogSuccess)
      try {
        while (dfResultLogSuccess.rs.next()) {
          val errMsg = s"Found Status SUCCEED Job on ictrl_dt: $ictrlDtUpdate Running ON -> dag_run_id: ${dfResultLogSuccess.rs.getString("dag_run_id")} round_time: ${dfResultLogSuccess.rs.getTimestamp("round_time")}"
          println(errMsg)

          println("-- Has Log Success in ingest audit log --")

          val queryDeleteLog =
            s"""
      DELETE FROM $ingestAuditLogsTable
      WHERE job_nm = '$jobNmUpdate' AND tasksgroup_nm = '$tasksgroupNmUpdate' AND round_time = '$roundTime' AND dag_run_id = '$dagRunId'
      """
          ConnectionService.postgresqlInsertUpdateFunc(
            connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
            connectionInfo.getUserNm, connectionInfo.getPassword, queryDeleteLog
          )
          println("Delete audit log Complete")
          throw new DropSuccessJobError(errMsg)
        }
      }
      finally{
        dfResultLogSuccess.close()
      }
    }
  }

//  def doGetDelayDayStatus(postgresConnectionInfo: ConnectionInfo,
//                          currentLocalDateRun: LocalDateTime, jobName: String,
//                          refDateIctrlDt: String, dateFormatIctrlDtForTb: String): String = {
//    val queryIngestionLogs = f"select * from $schemaName.tbl_ingest_audit_logs where lower(job_nm) = lower('${jobName}')"
//    val ingestionLogJobs = ConnectionService.postgresqlQueryFunc(postgresConnectionInfo.getIp, postgresConnectionInfo.getPort,
//      postgresConnectionInfo.getDbName, postgresConnectionInfo.getUserNm, postgresConnectionInfo.getPassword, queryIngestionLogs,sparkSession)
//    val currentLocalDate = currentLocalDateRun.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
//    val getLogSuccessPostgresql =  f"""SELECT job_nm, job_start_time, status
//        FROM (
//            SELECT
//                job_nm,
//                status,
//                job_start_time,
//                ROW_NUMBER() OVER (PARTITION BY job_start_time ORDER BY job_start_time DESC) AS rn
//            FROM fwconfz.tbl_ingest_audit_logs
//            WHERE
//                job_start_time BETWEEN
//                    (TO_TIMESTAMP('$currentLocalDate 00:00:00', 'YYYY-MM-DD HH24:MI:SS') - INTERVAL '1 day')
//                    AND TO_TIMESTAMP('$currentLocalDate 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
//                AND status = 'SUCCEED'
//                AND job_nm = 'check_job_nm'
//        ) a
//        WHERE rn = 1
//        ORDER BY job_start_time DESC
//        LIMIT 1"""
//    val ingestionLogsSuccess = ConnectionService.postgresqlQueryFunc(
//      postgresConnectionInfo.getIp, postgresConnectionInfo.getPort,
//      postgresConnectionInfo.getDbName, postgresConnectionInfo.getUserNm,
//      postgresConnectionInfo.getPassword,
//      getLogSuccessPostgresql,sparkSession)
//    val delayTimeResult = delayTime(sparkSession, jobName,refDateIctrlDt,
//      currentLocalDate,null, dateFormatIctrlDtForTb)
//    val logSuccessCount = ingestionLogsSuccess.count()
//    var delayDayStatus = "-"
//    if(delayTimeResult._1.isEmpty && delayTimeResult._2 == "-") {
//
//    }
//    else if(delayTimeResult._1.head == "Delay flag is off"
//      && delayTimeResult._2 == "Delay flag is off") {
//
//    }
//    else if(ingestionLogJobs.isEmpty) {
//      if(!delayTimeResult._1.contains(currentLocalDate)) {
//        delayDayStatus = "Not a Date to do this job"
//      }
//    }
//    else if(logSuccessCount == 1 && delayTimeResult._1.contains(currentLocalDate)) {
//      delayDayStatus = "Job Has already Done"
//    }
//    else if(!delayTimeResult._1.contains(currentLocalDate)) {
//      delayDayStatus = "Not a Date to do this job"
//    }
//    delayDayStatus
//  }

  override def doRunFramework(dependencyCheckModel: DependencyCheckModel,
                              JOB_TYPE: JobConstant.JOB_TYPE,jobName: String,
                              controlJobDf: Row,tblConfName: String,
                              httpServletRequest: HttpServletRequest,
                              username: String,roundTime: LocalDateTime): CompletableFuture[ExecuteResponse] = {
    val completableFuture: CompletableFuture[ExecuteResponse] = CompletableFuture.supplyAsync(new Supplier[ExecuteResponse] {
      override def get(): ExecuteResponse =  {
        {
          dependencyCheckModel.set_bldStartDate(new Timestamp(System.currentTimeMillis()))
          if (dependencyCheckModel.getModuleNotebookName == null)
            dependencyCheckModel.setModuleNotebookName("zeppelin-se-uat-g")
          var masterRefDate: LocalDateTime = null
          val frequency = controlJobDf.getAs[String]("frequency")
          var backdate: Any = 0
          if(!JOB_TYPE.equals(JobConstant.JOB_TYPE.KAFKA))
            backdate = controlJobDf.getAs[Any]("back_day")
          var currentLocalDateRun: LocalDateTime = null
          val taskGroupName = controlJobDf.getAs[String]("tasksgroup_nm")
          val catchUpType = JobConstant.CATCHUP_TYPE.SEQUENCE.getValue
          var ictrlDtTgtFmt = controlJobDf.getAs[String]("ictrl_dt_tgtfmt")
          if(ictrlDtTgtFmt == null) {
            ictrlDtTgtFmt = "%Y%m%d"
          }
          val dateFormatIctrlDtForTb = convertPythonDateFormatToJava(ictrlDtTgtFmt)
          val tableName = controlJobDf.getAs[String]("target_table_nm")
          val loadType = controlJobDf.getAs[String]("load_type")
          val timeRetry = controlJobDf.getAs[Int]("time_retry")
          val totalRetry = controlJobDf.getAs[Int]("total_retry")
          val schemaNameFromTbl = controlJobDf.getAs[String]("target_schema_nm")
          var currentDateRun = controlJobDf.getAs[String]("last_success_ictrl_dt")
          val lastSuccessIctrlDt = currentDateRun
          var processJobType: String = "ongoing"
          var origRefDate: LocalDateTime = null
          if (dependencyCheckModel.getFixedDate == null || dependencyCheckModel.getFixedDate.isEmpty) {
            masterRefDate = minusDateByFrequency(
              truncateToFormat(dependencyCheckModel.get_bldEndDate().
                toLocalDateTime,dateFormatIctrlDtForTb),frequency,backdate)
            origRefDate =  dependencyCheckModel.get_bldEndDate().toLocalDateTime
          }
          else {
            val tempDateTime = parseToLocalDateTime(dependencyCheckModel.getFixedDate, dateFormatIctrlDtForTb).get
            masterRefDate = truncateToFormat(tempDateTime,dateFormatIctrlDtForTb)
            origRefDate = tempDateTime
            processJobType = "manual"
          }
          if (currentDateRun == null || loadType.equals(LOAD_TYPE.FULL_LOAD.getValue)) {
            currentLocalDateRun = masterRefDate
            logger.info("current local date run init = {}",currentDateRun)
            currentDateRun = masterRefDate.format(DateTimeFormatter.ofPattern(dateFormatIctrlDtForTb))
          }
          else {
            currentLocalDateRun = parseToLocalDateTime(currentDateRun, dateFormatIctrlDtForTb).get
            if (catchUpType.equals(CATCHUP_TYPE.SEQUENCE.getValue)) {
              logger.info("check increase value")
              currentLocalDateRun = addDateByFrequency(currentLocalDateRun, frequency)
              logger.info("check after increase current local date = {}",currentLocalDateRun);
            }
            if(currentLocalDateRun.isAfter(masterRefDate)) {
              currentLocalDateRun = masterRefDate
            }
            logger.info("check current local date run = {}",currentLocalDateRun)
          }
          var isCdr: Boolean = false
          if (JOB_TYPE == JobConstant.JOB_TYPE.FILE) {
            isCdr = "Y".equalsIgnoreCase(controlJobDf.getAs[String]("cdr_flag"))
          }
          if (isCdr) {
            currentLocalDateRun = masterRefDate
          }
          var startIctrlDt = currentLocalDateRun.format(DateTimeFormatter.ofPattern(dateFormatIctrlDtForTb))
          var startRefDate = currentLocalDateRun
          if(loadType.equals(LOAD_TYPE.UPSERT.getValue)) {
            currentLocalDateRun = masterRefDate
            currentDateRun =  masterRefDate.format(
              DateTimeFormatter.ofPattern(dateFormatIctrlDtForTb))
          }
          val sb = new StringBuilder()
          var overlap: String = "0"
          if (JOB_TYPE != JobConstant.JOB_TYPE.KAFKA) {
            overlap = if(controlJobDf.getAs[Any]("overlap") != null) {
              controlJobDf.getAs[Any]("overlap").toString
            }
            else {
              "0"
            }
          }
          var isContinueRunning: Boolean = true
          var ictrlDtRun: LocalDateTime = null
          val queryMasterSql = s"SELECT system,key,values FROM $schemaName.tbl_master_config where system = 'fw_postgre'"
          val postgresConnectionInfo = ConnectionService.getMasterConfigLog(queryMasterSql, salt, ultKey)
          val executeResponse: ExecuteResponse = new ExecuteResponse
          executeResponse.setJobName(jobName)
          breakable {
            while (isContinueRunning) {
              var runId = ""
              if (dependencyCheckModel.get_workflowId() != null) {
                runId = dependencyCheckModel.get_workflowId() + "|" + dependencyCheckModel.get_runId() + "|" + dependencyCheckModel.get_taskId()
              }
              else {
                runId = "manual_run_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HH_mm_ss"))
              }
              ictrlDtRun = masterRefDate
              if (catchUpType.equalsIgnoreCase(CATCHUP_TYPE.SEQUENCE.getValue)) {
                logger.info("current local date run = {}",currentLocalDateRun)
                logger.info("master ref date = {}",masterRefDate)
                if (masterRefDate.isAfter(currentLocalDateRun) || masterRefDate.isEqual(currentLocalDateRun)) {
                  ictrlDtRun = currentLocalDateRun
                }
                else if (currentLocalDateRun.isAfter(masterRefDate)) {
                  isContinueRunning = false
                  break()
                }
              }
              else {
                isContinueRunning = false
              }
              val refDateIctrlDt = ictrlDtRun.format(DateTimeFormatter.ofPattern(dateFormatIctrlDtForTb))
              if(!loadType.equals(LOAD_TYPE.UPSERT.getValue)) {
                startRefDate = ictrlDtRun
                startIctrlDt = refDateIctrlDt
              }
              val startDateWithOverlap = calOverLap(startRefDate, overlap, frequency)
              val startDateWithOverlapIctrlDt = startDateWithOverlap.format(DateTimeFormatter.ofPattern(dateFormatIctrlDtForTb))
              val runTime: LocalDateTime = LocalDateTime.now()
              insertRoundAuditLog(dependencyCheckModel,
                controlJobDf.getAs[String]("target_schema_nm"), controlJobDf.getAs[String]("target_table_nm"),
                runId, refDateIctrlDt, postgresConnectionInfo, runTime, startDateWithOverlapIctrlDt, refDateIctrlDt,
                roundTime,controlJobDf.getAs[String]("load_type"),controlJobDf.getAs[String]("ingestion_type"),
                taskGroupName,jobName)
              try {
                var jobEndTime: LocalDateTime = null
                checkRunningIctrlDtIngest(jobName,
                  taskGroupName, refDateIctrlDt, roundTime, runTime, startDateWithOverlapIctrlDt,
                  refDateIctrlDt, processJobType, isCdr, runId,
                  sparkSession.sparkContext.applicationId, frequency,
                  postgresConnectionInfo,dateFormatIctrlDtForTb)
                var startDetailTime: LocalDateTime = LocalDateTime.now()
                val queryMasterSourceSql = s"SELECT system,key,values FROM $schemaName.tbl_master_config where system = 'core_dwh_TEDWHDPAPPB'"
                val connectionInfo = ConnectionService.getMasterConfigLog(queryMasterSourceSql, salt, ultKey)
                breakable {
                  for (i <- 0 to totalRetry) {
                    val results: java.util.Map[String, Boolean] =
                      checkDependencyByJobName(controlJobDf, ictrlDtRun, postgresConnectionInfo,
                        refDateIctrlDt, currentDateRun, connectionInfo, jobName,null)
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
                    if (hasNotReadyJob) {
                      if (i == totalRetry - 1)
                        throw new InvalidArgumentException("The dependency job check failed because this job = " + String.join(",", notReadyJob) + " is not finished")
                    }
                    else {
                      break()
                    }
                    updateJobStatusAndErrorMessage(jobName,roundTime,runId,
                      "tbl_ingest_audit_logs",postgresConnectionInfo,
                      "WAITING","wailting depen",refDateIctrlDt)
                    Thread.sleep(timeRetry * 1000)
                    updateJobStatusAndErrorMessage(jobName,roundTime,runId,
                      "tbl_ingest_audit_logs",postgresConnectionInfo,"running",null,refDateIctrlDt)
                  }
                }
                var stepRun = "FW CHECK PREREQUISITE"
                var stepSeq = "FW:1"
                var stepRunNext = "FW RUN SCRIPTS INGESTION"
                var stepSeqNext = "FW:2"
                insertAuditLogDetail(stepRun,stepSeq,jobName,runId,taskGroupName,schemaNameFromTbl,
                  tableName,loadType,roundTime,startDetailTime,"SUCCESS",refDateIctrlDt,
                  stepRunNext,stepSeqNext,postgresConnectionInfo,"tbl_ingest_audit_detail_logs",
                  "tbl_ingest_audit_detail_next_logs")
                startDetailTime = LocalDateTime.now()
                val param: java.util.HashMap[String, JsonNode] = new util.HashMap[String, JsonNode]()
                param.put("job_nm", objectMapper.valueToTree(jobName))
                param.put("tasksgroup_nm", objectMapper.valueToTree(taskGroupName))
                param.put("rl_ref_date", objectMapper.valueToTree(ictrlDtRun.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"))))
                param.put("ictrl_dt", objectMapper.valueToTree(refDateIctrlDt))
                param.put("start_ictrl_dt", objectMapper.valueToTree(startIctrlDt))
                param.put("end_ictrl_dt", objectMapper.valueToTree(refDateIctrlDt))
                param.put("start_ictrl_dt_w_overlap", objectMapper.valueToTree(startDateWithOverlapIctrlDt))
                param.put("end_ictrl_dt_w_overlap", objectMapper.valueToTree(refDateIctrlDt))
                if(controlJobDf.getAs[String]("ictrl_dt_type") != null)
                  param.put("ictrl_dt_type", objectMapper.valueToTree(controlJobDf.getAs[String]("ictrl_dt_type")))
                else
                  param.put("ictrl_dt_type", objectMapper.valueToTree("system"))
                param.put("ictrl_dt_tgtfmt", objectMapper.valueToTree(ictrlDtTgtFmt))
                param.put("frequency_job", objectMapper.valueToTree(frequency))
                param.put("load_type", objectMapper.valueToTree(controlJobDf.getAs[String]("load_type")))
                param.put("table_conf", objectMapper.valueToTree(tblConfName))
                if(dependencyCheckModel.getFixedDate != null)
                  param.put("manual_ref_date", objectMapper.valueToTree("False"))
                else
                  param.put("manual_ref_date",objectMapper.valueToTree("True"))
                param.put("last_success_ictrl_dt",objectMapper.valueToTree(currentDateRun))
                param.put("dag_run_id", objectMapper.valueToTree(runId))
                if (dependencyCheckModel.getTaskGroupName != null)
                  param.put("job_run_mode", objectMapper.valueToTree("tasksgroup"))
                else
                  param.put("job_run_mode", objectMapper.valueToTree("job"))
                param.put("round_time", objectMapper.valueToTree(roundTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"))))
                val jobStartTime: LocalDateTime = LocalDateTime.now()
                updateJobStartTimeOfAuditLogByJobNameAndRoundTimeAndDagRun(
                  jobName,jobStartTime,roundTime,runId,
                  "tbl_ingest_audit_logs",postgresConnectionInfo,refDateIctrlDt)
                var runNotebookParallelResult: RunNotebookParallelResult = null
                if(dependencyCheckModel.getWorkspaceName != null) {
                  runNotebookParallelResult =
                    doRunNotebookParallel(param, f"2M4GWV7SQ~${dependencyCheckModel.getWorkspaceName}", dependencyCheckModel,
                      username, runId, httpServletRequest)
                }
                else {
                  runNotebookParallelResult =
                    doRunNotebookParallel(param, "2M4GWV7SQ", dependencyCheckModel,
                      username, runId, httpServletRequest)
                }
                var status = ""
                if (runNotebookParallelResult.getErrorMsg != null) {
                  status = "FAILED"
                  postProcess(status, dependencyCheckModel,
                    runNotebookParallelResult.getErrorSpecificMsg,
                    runId, sparkSession,
                    runNotebookParallelResult.getNotebookUrl, jobStartTime,
                    LocalDateTime.now(), roundTime, postgresConnectionInfo, refDateIctrlDt, jobName,
                    "tbl_ingest_audit_logs", tblConfName,lastSuccessIctrlDt)
                  throw new RunNotebookParallelException(runNotebookParallelResult.getErrorMsg)
                }
                jobEndTime = LocalDateTime.now()
                status = "SUCCEED"
                stepRun = "FW RUN SCRIPTS INGESTION"
                stepSeq = "FW:2"
                stepRunNext = null
                stepSeqNext = null
                insertAuditLogDetail(stepRun,stepSeq,jobName,runId,taskGroupName,schemaNameFromTbl,
                  tableName,loadType,roundTime,startDetailTime,"SUCCEED",refDateIctrlDt,
                  stepRunNext,stepSeqNext,postgresConnectionInfo,"tbl_ingest_audit_detail_logs",
                  "tbl_ingest_audit_detail_next_logs")
                postProcess(status, dependencyCheckModel, runNotebookParallelResult.getErrorSpecificMsg,
                  runId, sparkSession, runNotebookParallelResult.getNotebookUrl, jobStartTime,
                  jobEndTime, roundTime, postgresConnectionInfo, refDateIctrlDt, jobName,
                  "tbl_ingest_audit_logs", tblConfName,lastSuccessIctrlDt)
                sb.append(runNotebookParallelResult.getMessage)
                if (catchUpType.equalsIgnoreCase(CATCHUP_TYPE.SEQUENCE.getValue)) {
                  currentLocalDateRun = addDateByFrequency(currentLocalDateRun, frequency)
                  if (currentLocalDateRun.isAfter(masterRefDate)) {
                    isContinueRunning = false
                  }
                }
              }
              catch {
                case exception: Exception => {
                  logger.error(exception.getMessage,exception)
                  if(!exception.getClass.equals(classOf[RunNotebookParallelException])) {
                    updateStateOfAuditLogByJobNameAndRoundTimeAndDagRun(
                      "FAILED",
                      dependencyCheckModel,runId,LocalDateTime.now(),
                      roundTime,postgresConnectionInfo,exception.getMessage,
                      "tbl_ingest_audit_logs",jobName,refDateIctrlDt)
                  }
                  throw new Exception(exception.getMessage)
                }
              }
            }
          }
          executeResponse.setMessage(sb.toString())
          executeResponse
        }
      }
    },taskExecutor)
    completableFuture
  }

  def insertRoundAuditLog(dependencyCheckModel: DependencyCheckModel,
                          schemaName: String, tableName: String, runId: String,
                          ictrlDt:String,connectionInfo: ConnectionInfo,jobStartTime: LocalDateTime,
                          startIctrlDt:String,endIctrlDt: String,
                          roundTime: LocalDateTime,loadType:String,ingestType:String,
                          taskGroupName:String,jobName:String): Unit = {
    val seqValue = Seq(jobName,roundTime,runId,
      schemaName,tableName,jobStartTime,ictrlDt,startIctrlDt,endIctrlDt,
      dependencyCheckModel.getModuleNotebookName,loadType,ingestType,taskGroupName)
    val sql = f"insert into ${this.schemaName}.tbl_ingest_audit_logs (job_nm,round_time," +
      f"dag_run_id,target_schema_nm,target_table_nm," +
      f"custom_start_time,ictrl_dt,start_ictrl_dt," +
      f"end_ictrl_dt,status,zeppelin,load_type,ingestion_type,tasksgroup_nm) values (" +
      f"?,?,?,?,?,?,?," +
      f"?,?,'RUNNING'," +
      f"?,?,?,?)"
    ConnectionService.postgresqlInsertUpdateFunc(connectionInfo.getIp,connectionInfo.getPort,connectionInfo.getDbName,
      connectionInfo.getUserNm,connectionInfo.getPassword,sql,seqValue)
  }

  def checkOrderDateTimeFormat(pattern_date: String): Boolean = {
    val pythonToPyspark = Map(
      "%Y" -> "YYYY",
      "%y" -> "YY",
      "%m" -> "MM",
      "%B" -> "MMMM",
      "%b" -> "MMM",
      "%d" -> "DD",
      "%H" -> "HH",
      "%I" -> "hh",
      "%M" -> "mm",
      "%S" -> "ss",
      "%f" -> "SSS",
      "%a" -> "EEE",
      "%A" -> "EEEE",
      "%w" -> "e",
      "%p" -> "a",
      "%z" -> "Z",
      "%Z" -> "z"
    )

    var use_between_flag = true
    val list_order_to_check = List(List("%Y", "%y"), List("%U", "%W"), "%m", "%w", "%d", "%H", "%M", "%S", "%f")
    var current_index = 0
    var pattern_date_temp = pattern_date.replace("%%", "")

    def check_index_and_found(format_in: String, char_find: String): Int = {
      var index_on_find = -1
      var max_index_on_find = -1
      var temp_format = format_in
      while (temp_format.indexOf(char_find) != -1) {
        index_on_find = temp_format.indexOf(char_find)
        if (index_on_find != -1) {
          max_index_on_find = max_index_on_find.max(index_on_find)
        }
        temp_format = temp_format.replaceFirst(Regex.quote(char_find), "")
      }
      max_index_on_find
    }

    for (item_in <- list_order_to_check) {
      if (!use_between_flag) {
        // Not Check
        return false
      }

      item_in match {
        case item_list: List[String] =>
          var checking_loop_count = 0
          for (item <- item_list) {
            val index_out = check_index_and_found(pattern_date_temp, item)
            if (index_out != -1) {
              if (index_out >= current_index) {
                current_index = index_out
              } else {
                if (checking_loop_count == 0) {
                  use_between_flag = false
                  return false
                }
              }
            }
            checking_loop_count = checking_loop_count + 1
            pattern_date_temp = pattern_date_temp.replace(item, "")
          }
        case item_str: String =>
          val index_out = check_index_and_found(pattern_date_temp, item_str)
          if (index_out != -1) {
            if (index_out >= current_index) {
              current_index = index_out
            } else {
              use_between_flag = false
              return false
            }
          }
          pattern_date_temp = pattern_date_temp.replace(item_str, "")
      }
    }

    if (pattern_date_temp.contains('%')) {
      use_between_flag = false
    }

    println(s"Status Check Flag: $use_between_flag")
    use_between_flag
  }

  def oracleIngestionQueryLog(spark: SparkSession, server: String, port: String, username: String, password: String, query_string: String, sid: String): DataFrame = {
    logger.info("oracle url = {}",s"jdbc:oracle:thin:@$server:$port/$sid")
    try {
      val spdf = spark.read.format("jdbc")
        .option("url", s"jdbc:oracle:thin:@$server:$port/$sid")
        .option("query", query_string)
        .option("user", username)
        .option("password", password)
        .option("driver", "oracle.jdbc.OracleDriver")
        .option("encrypt", "true")
        .option("trustServerCertificate", "true")
        .load()
      println("Complete Query Datafrom Oracle.")
      spdf
    } catch {
      case e: Exception =>
        throw new InvalidArgumentException(e.toString)
    }
  }

  def queryOracle(prerequisiteJobNameStr: String, patternIctrlDtCheck: String,
                  prerequisiteTableCleaned: String,prerequisiteSchema: String,
                  targetDate: String, depenIp: String, depenPort: String,
                  depenUserNm: String, depenPassword: String, depenSid: String,
                  prerequisiteTable: String): (Boolean, Map[String, List[String]]) = {
    val conditionWhereJobNm = if (prerequisiteJobNameStr.isEmpty) "" else s"AND WORKFLOW_NM IN ('$prerequisiteJobNameStr')"
    val targetFormatDate = convertPythonToPysparkFormat(patternIctrlDtCheck)
    val dbDailyQuery =
      s"""
         |with INFORMATICA_DELTA
         |as (select distinct FOLDER_NM,WORKFLOW_NM,TGT_OBJ
         |   from TEDWAUXAPPO.AUX_DELTA
         |   where OBS_IND = 0 AND TGT_OBJ = UPPER('$prerequisiteTableCleaned')
         |   AND WORKFLOW_TYPE NOT IN ('EXP_TO_DATAMART','MANUAL')
         |   AND WORKFLOW_NM not in ( 'WF_DIM_ORDR_ACTVTN_UPDATE_HRCHY','WF_DIM_ACCT_SAP_MAN') $conditionWhereJobNm
         |   AND DECODE( FOLDER_NM
         |     , '@ANA_SHARE', 'TEDWANAAPPO'
         |     , '@AUX_SHARE', 'TEDWAUXAPPO'
         |     , '@CDS_SHARE', 'CDSAPPO'
         |     , '@CHNLMGMT_SHARE', 'CHNLMGMTAPPO'
         |     , '@CLM_SHARE', 'CLMAPPO'
         |     , '@COMM_SHARE', 'COMMONAPPO'
         |     , '@CORP_SHARE', 'CORPAPPO'
         |     , '@DDA_SHARE', 'DDAACSAPPO'
         |     , '@DMT_SHARE_DMT', 'TEDWDMTAPPO'
         |     , '@DMT_SHARE_TDA', 'TEDWTDAAPPO'
         |     , '@DTH_SHARE', 'TAMBOLAPPO'
         |     , '@EDW_SHARE_ACS', 'TEDWACSAPPO'
         |     , '@EDW_SHARE_BI', 'TEDWBIAPPO'
         |     , '@EDW_SHARE_CIS', 'TEDWCISAPPO'
         |     , '@EDW_SHARE_DWH', 'TEDWDWHAPPO'
         |     , '@EDW_SHARE_IMG', 'TEDWIMGAPPO'
         |     , '@EDW_SHARE_LOG', 'TSIDLOGAPPO'
         |     , '@EDW_SHARE_MMONEY', 'MMONEYAPPO'
         |     , '@EDW_SHARE_STAGING', 'STAGING'
         |     , '@EDW_SHARE_STG', 'TEDWSTGAPPO'
         |     , '@EDW_SHARE_STGAPPO', 'TEDWSTGAPPO'
         |     , '@FIN_SHARE', 'FIN1SBOX'
         |     , '@GEOSPC_SHARE', 'GEOSPCAPPO'
         |     , '@IOT_SHARE', 'IOTAPPO'
         |     , '@PRODPERF_SHARE', 'PRODPERFAPPO'
         |   ) = UPPER('$prerequisiteSchema'))
         |SELECT *
         |FROM (select TGT_OBJ as SOURCE_OBJECT,WORKFLOW_NM,
         |   NVL((select distinct 1
         |   from TEDWAUXAPPO.AUX_PROCESS_HIST
         |   where to_date('$targetDate','$targetFormatDate') <= trunc(XTR_END_DT) and STAT = 'Succeeded'
         |   and FOLDER_NM = TD.FOLDER_NM and WORKFLOW_NM = TD.WORKFLOW_NM
         |   ),0) as TRIGGET
         |from INFORMATICA_DELTA TD
         |ORDER BY 3
         |) TD
         |WHERE rownum = 1
         |""".stripMargin
    logger.info("sql = {}",dbDailyQuery)
    val records = oracleIngestionQueryLog(sparkSession,depenIp, depenPort, depenUserNm, depenPassword, dbDailyQuery, depenSid).collect()
    if (records.nonEmpty) {
      for (record <- records) {
        // Access columns by index (0-based)
        val sourceObject = record.getString(0)
        val trigger = record.getDecimal(2)

        val isReadyDepen = sourceObject.equalsIgnoreCase(prerequisiteTable) && trigger.intValue() == 1

        return (isReadyDepen, Map(prerequisiteJobNameStr -> List(targetDate)))
      }
      (false,Map(prerequisiteJobNameStr -> List(targetDate)))
    } else {
      (false, Map(prerequisiteJobNameStr -> List(targetDate)))
    }
  }

  def delayTime(
                 spark: SparkSession,
                 jobNm: String,
                 refDateWorkflow: String,
                 businessDate: String,
                 dbConfig: String = "",
                 patternRefDate: String = "yyyyMMdd"
               ): (Seq[String], String) = {
    Try {

      val tblConfigSchema = s"$schemaName.tbl_job_control_delays"
      val refDate = LocalDate.parse(refDateWorkflow, DateTimeFormatter.ofPattern(patternRefDate))

      val dfDelay = spark.sql(s"SELECT job_nm, job_type, date_start_job, total_delay, mode, flag FROM $tblConfigSchema WHERE lower(job_type) = 'ingestion'")

      val dfDelayJob = dfDelay.filter(col("job_nm") === jobNm && lower(col("flag")) =!= "n")

      if (dfDelayJob.isEmpty) {
        println(s"Job name $jobNm : Has no delay days or flag is off")
        (Seq.empty, "-")
      } else {
        val delayConfig = dfDelayJob.head()
        val jobNmDelay = delayConfig.getAs[String]("job_nm")
        val jobType = delayConfig.getAs[String]("job_type")
        val dateStartJob = delayConfig.getAs[String]("date_start_job")
        val totalDelay = delayConfig.getAs[String]("total_delay").toInt
        val mode = delayConfig.getAs[String]("mode")
        val flag = delayConfig.getAs[String]("flag")

        mode.toLowerCase match {
          case "week" =>
            val dayOfWeekIndex = Seq("Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday")
            val indexFirstDay = dayOfWeekIndex.indexWhere(day => dateStartJob.toLowerCase.contains(day.toLowerCase))

            val startOfWeek = refDate.minusDays(refDate.getDayOfWeek.getValue % 7)
            val firstDelayDay = startOfWeek.plusDays(indexFirstDay)

            val datesToRun = (0 until totalDelay).map { i =>
              firstDelayDay.plusDays(i).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
            }
            (datesToRun, datesToRun.lastOption.getOrElse("-"))

          case "month" =>
            val dateWorkflowDay = refDateWorkflow.takeRight(2).toInt
            val startDay = dateStartJob.toInt

            val baseDate = if (startDay > dateWorkflowDay) {
              refDate.withDayOfMonth(startDay).minusMonths(1)
            } else {
              refDate.withDayOfMonth(startDay)
            }

            val datesToRun = (0 until totalDelay).map { i =>
              baseDate.plusDays(i).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
            }
            (datesToRun, datesToRun.lastOption.getOrElse("-"))

          case _ =>
            println(s"Unsupported mode: $mode")
            (Seq.empty, "-")
        }
      }
    } match {
      case Success(value) => value
      case Failure(e) =>
        // Assuming there's a custom exception class similar to DelayTimeError
        // throw new DelayTimeError(e)
        throw e // Re-throwing the original exception for now
    }
  }

  def convertPythonToPysparkFormat(pyFormat: String): String = {
    val pythonToPyspark = Map(
      "%Y" -> "yyyy", "%y" -> "yy", "%m" -> "MM", "%B" -> "MMMM", "%b" -> "MMM",
      "%d" -> "dd", "%H" -> "HH", "%I" -> "hh", "%M" -> "mm", "%S" -> "ss",
      "%f" -> "SSS", "%a" -> "EEE", "%A" -> "EEEE", "%w" -> "e", "%p" -> "a",
      "%z" -> "Z", "%Z" -> "z"
    )
    pythonToPyspark.foldLeft(pyFormat)((current, entry) => current.replace(entry._1, entry._2))
  }

  def checkLogIngestion(prerequisiteJobNm: Any, prerequisiteSchema: String, prerequisiteTable: String,
                        frequencyCheck: String, values: Option[String], emptyFlag: Int,
                        businessColumn: String, patternIctrlDtCheck: String,
                        patternIctrlDt: String = "", masterRefDate: LocalDateTime,
                        logTable: String = "tbl_ingest_logs", depenIp: String = "", depenPort: String = "",
                        depenUserNm: String = "", depenPassword: String = "", depenSid: String = "",
                        postgresConnectionInfo:ConnectionInfo): (Boolean, Map[String, List[String]]) = {

    try {
      val checkInDate = if (businessColumn == "None" || businessColumn == null) "logs" else "rawDate"
      println(s"Checking Type: $checkInDate")

      val tblIngestAuditLogs = s"$schemaName.tbl_ingest_audit_logs"
      val tblTauditLogs = s"$schemaName.tbl_trans_audit_logs"

      val prerequisiteJobNameStr = prerequisiteJobNm match {
        case l: List[String] => l.filter(s => s != null && s.nonEmpty && s.toLowerCase != "none").map(_.toUpperCase).mkString("', '")
        case s: String => if (s.toLowerCase == "none" || s.isEmpty) "" else s.toUpperCase
        case _ => ""
      }

      val prerequisiteTableCleaned = prerequisiteTable.split(" ")(0)
      logger.info("tbl ingest audit logs = {}",tblIngestAuditLogs)
      val baseQuery = s"select target_table_nm, job_start_time, ictrl_dt, status, row_cnt from $tblIngestAuditLogs " +
        s"where upper(job_nm) = upper('$prerequisiteJobNameStr') and target_schema_nm = '$prerequisiteSchema' and target_table_nm = '$prerequisiteTableCleaned'"

      frequencyCheck match {
        case "daily" =>
          val targetDate = masterRefDate.format(DateTimeFormatter.ofPattern(patternIctrlDtCheck))
          val query = s"$baseQuery and ictrl_dt like '$targetDate%' order by job_start_time desc"
          logger.info("check query daily ingest sql depen = {}",query)
          if (checkInDate == "logs" && ((
            prerequisiteJobNm.isInstanceOf[String] || prerequisiteJobNm.asInstanceOf[List[String]].size == 1) &&
            prerequisiteJobNameStr != null && !blackListNullString.contains(prerequisiteJobNameStr))) {
            val result = ConnectionService.postgresqlQueryDirectly(postgresConnectionInfo.getIp, postgresConnectionInfo.getPort,
              postgresConnectionInfo.getDbName, postgresConnectionInfo.getUserNm,
              postgresConnectionInfo.getPassword,query)
            try {
              val map = checkExistsDataWithOutCheckMiss(result.rs, targetDate, prerequisiteJobNameStr, emptyFlag, prerequisiteTableCleaned)
              if(!map._1) {
                queryOracle(prerequisiteJobNameStr, patternIctrlDtCheck, prerequisiteTableCleaned, prerequisiteSchema, targetDate, depenIp, depenPort, depenUserNm, depenPassword, depenSid, prerequisiteTable)
              }
              else {
                map
              }
            }
            finally {
              result.close()
            }
          } else if (checkInDate == "rawDate") {
            val result = checkBusinessColumn(frequencyCheck, businessColumn, prerequisiteSchema, prerequisiteTableCleaned, targetDate,sparkSession)
            if(result.isEmpty) {
              queryOracle(prerequisiteJobNameStr, patternIctrlDtCheck, prerequisiteTableCleaned, prerequisiteSchema, targetDate, depenIp, depenPort, depenUserNm, depenPassword, depenSid, prerequisiteTable)
            }
            else {
              (true, Map(prerequisiteJobNameStr -> List()))
            }
          } else {
            // Case for Oracle dependency check
            val conditionWhereJobNm = if (prerequisiteJobNameStr.isEmpty) "" else s"AND WORKFLOW_NM IN ('$prerequisiteJobNameStr')"
            val targetFormatDate = convertPythonToPysparkFormat(patternIctrlDtCheck)
            val dbDailyQuery =
              s"""
                 |with INFORMATICA_DELTA
                 |as (select distinct FOLDER_NM,WORKFLOW_NM,TGT_OBJ
                 |   from TEDWAUXAPPO.AUX_DELTA
                 |   where OBS_IND = 0 AND TGT_OBJ = UPPER('$prerequisiteTableCleaned')
                 |   AND WORKFLOW_TYPE NOT IN ('EXP_TO_DATAMART','MANUAL')
                 |   AND WORKFLOW_NM not in ( 'WF_DIM_ORDR_ACTVTN_UPDATE_HRCHY','WF_DIM_ACCT_SAP_MAN') $conditionWhereJobNm
                 |   AND DECODE( FOLDER_NM
                 |     , '@ANA_SHARE', 'TEDWANAAPPO'
                 |     , '@AUX_SHARE', 'TEDWAUXAPPO'
                 |     , '@CDS_SHARE', 'CDSAPPO'
                 |     , '@CHNLMGMT_SHARE', 'CHNLMGMTAPPO'
                 |     , '@CLM_SHARE', 'CLMAPPO'
                 |     , '@COMM_SHARE', 'COMMONAPPO'
                 |     , '@CORP_SHARE', 'CORPAPPO'
                 |     , '@DDA_SHARE', 'DDAACSAPPO'
                 |     , '@DMT_SHARE_DMT', 'TEDWDMTAPPO'
                 |     , '@DMT_SHARE_TDA', 'TEDWTDAAPPO'
                 |     , '@DTH_SHARE', 'TAMBOLAPPO'
                 |     , '@EDW_SHARE_ACS', 'TEDWACSAPPO'
                 |     , '@EDW_SHARE_BI', 'TEDWBIAPPO'
                 |     , '@EDW_SHARE_CIS', 'TEDWCISAPPO'
                 |     , '@EDW_SHARE_DWH', 'TEDWDWHAPPO'
                 |     , '@EDW_SHARE_IMG', 'TEDWIMGAPPO'
                 |     , '@EDW_SHARE_LOG', 'TSIDLOGAPPO'
                 |     , '@EDW_SHARE_MMONEY', 'MMONEYAPPO'
                 |     , '@EDW_SHARE_STAGING', 'STAGING'
                 |     , '@EDW_SHARE_STG', 'TEDWSTGAPPO'
                 |     , '@EDW_SHARE_STGAPPO', 'TEDWSTGAPPO'
                 |     , '@FIN_SHARE', 'FIN1SBOX'
                 |     , '@GEOSPC_SHARE', 'GEOSPCAPPO'
                 |     , '@IOT_SHARE', 'IOTAPPO'
                 |     , '@PRODPERF_SHARE', 'PRODPERFAPPO'
                 |   ) = UPPER('$prerequisiteSchema'))
                 |SELECT *
                 |FROM (select TGT_OBJ as SOURCE_OBJECT,WORKFLOW_NM,
                 |   NVL((select distinct 1
                 |   from TEDWAUXAPPO.AUX_PROCESS_HIST
                 |   where to_date('$targetDate','$targetFormatDate') <= trunc(XTR_END_DT) and STAT = 'Succeeded'
                 |   and FOLDER_NM = TD.FOLDER_NM and WORKFLOW_NM = TD.WORKFLOW_NM
                 |   ),0) as TRIGGET
                 |from INFORMATICA_DELTA TD
                 |ORDER BY 3
                 |) TD
                 |WHERE rownum = 1
                 |""".stripMargin
            logger.info("sql = {}",dbDailyQuery)
            val records = oracleIngestionQueryLog(sparkSession,depenIp, depenPort, depenUserNm, depenPassword, dbDailyQuery, depenSid).collect()
            if (records.nonEmpty) {
              for (record <- records) {
                // Access columns by index (0-based)
                val sourceObject = record.getString(0)
                val trigger = record.getDecimal(2)

                val isReadyDepen = sourceObject.equalsIgnoreCase(prerequisiteTable) && trigger.intValue() == 1

                return (isReadyDepen, Map(prerequisiteJobNameStr -> List(targetDate)))
              }
              (false,Map(prerequisiteJobNameStr -> List(targetDate)))
            } else {
              (false, Map(prerequisiteJobNameStr -> List(targetDate)))
            }
          }

        case "hourly" =>
          val hoursOffset = values.map(_.toInt).getOrElse(0)
          val targetDateTime = masterRefDate.minus(hoursOffset, ChronoUnit.HOURS)
          val targetDate = targetDateTime.format(DateTimeFormatter.ofPattern(patternIctrlDtCheck))
          val query = s"$baseQuery and ictrl_dt like '$targetDate%' order by job_start_time desc"
          println(s"Query: $query")

          if (checkInDate == "logs") {
            val records = ConnectionService.postgresqlQueryDirectly(postgresConnectionInfo.getIp, postgresConnectionInfo.getPort,
              postgresConnectionInfo.getDbName, postgresConnectionInfo.getUserNm,
              postgresConnectionInfo.getPassword, query)
            try {
              checkExistsDataWithOutCheckMiss(records.rs, targetDate, prerequisiteJobNameStr, emptyFlag, prerequisiteTableCleaned)
            }
            finally {
              records.close()
            }
          } else {
            val result = checkBusinessColumn(frequencyCheck, businessColumn, prerequisiteSchema, prerequisiteTableCleaned, targetDate,sparkSession)
            if(result.isEmpty) {
              val name = if (prerequisiteJobNameStr.nonEmpty) prerequisiteJobNameStr else prerequisiteTableCleaned
              (false, Map(name -> List(targetDate)))
            }
            else {
              (true, Map(prerequisiteJobNameStr -> List()))
            }
          }

        case "24hours" =>
          val dateRun = masterRefDate
          val dateStart = dateRun.withHour(0).withMinute(0).withSecond(0).withNano(0)
          val dateEnd = dateRun.withHour(23).withMinute(59).withSecond(59).withNano(0)

          val listDateTarget = (0 to 23).map { hour =>
            dateStart.plusHours(hour).format(DateTimeFormatter.ofPattern(patternIctrlDtCheck))
          }.toList

          val useBetweenQuery = checkOrderDateTimeFormat(patternIctrlDtCheck)
          val targetDateCondition = if (useBetweenQuery) {
            s"BETWEEN '${dateStart.format(DateTimeFormatter.ofPattern(patternIctrlDtCheck))}' AND '${dateEnd.format(DateTimeFormatter.ofPattern(patternIctrlDtCheck))}'"
          } else {
            s"IN (${listDateTarget.map(d => s"'$d'").mkString(", ")})"
          }

          val query = s"$baseQuery and ictrl_dt $targetDateCondition order by job_start_time desc"
          println(s"Query: $query")

          if (checkInDate == "logs") {
            val records = ConnectionService.postgresqlQueryDirectly(postgresConnectionInfo.getIp, postgresConnectionInfo.getPort,
              postgresConnectionInfo.getDbName, postgresConnectionInfo.getUserNm,
              postgresConnectionInfo.getPassword, query)
            try {
              checkExistsDataWithCheckMiss(records.rs, listDateTarget, prerequisiteJobNameStr, emptyFlag, null)
            }
            finally {
              records.close()
            }
          } else {
            val records = checkBusinessColumn(frequencyCheck, businessColumn, prerequisiteSchema, prerequisiteTableCleaned, targetDateCondition,sparkSession)
            val listLogDate = records.map(_.getString(0)).toList
            val findMiss = listDateTarget.toSet -- listLogDate.toSet
            if (findMiss.isEmpty) {
              println("Table is ready.")
              (true, Map(prerequisiteJobNameStr -> List()))
            } else {
              println("Table is not ready.")
              (false, Map(prerequisiteJobNameStr -> findMiss.toList))
            }
          }

        case "every_n_hour" =>
          val dateRun = masterRefDate
          val dateStart = dateRun.withHour(0).withMinute(0).withSecond(0).withNano(0)
          val dateEnd = dateRun.withHour(23).withMinute(59).withSecond(59).withNano(0)

          val listDateTarget = (0 to 23).map { hour =>
            dateStart.plusHours(hour).format(DateTimeFormatter.ofPattern(patternIctrlDtCheck))
          }.toList

          val useBetweenQuery = checkOrderDateTimeFormat(patternIctrlDtCheck)
          val targetDateCondition = if (useBetweenQuery) {
            s"BETWEEN '${dateStart.format(DateTimeFormatter.ofPattern(patternIctrlDtCheck))}' AND '${dateEnd.format(DateTimeFormatter.ofPattern(patternIctrlDtCheck))}'"
          } else {
            s"IN (${listDateTarget.map(d => s"'$d'").mkString(", ")})"
          }

          val query = s"$baseQuery and ictrl_dt $targetDateCondition order by job_start_time desc"
          println(s"Query: $query")

          if (checkInDate == "logs") {
            val records = ConnectionService.postgresqlQueryDirectly(postgresConnectionInfo.getIp, postgresConnectionInfo.getPort,
              postgresConnectionInfo.getDbName, postgresConnectionInfo.getUserNm,
              postgresConnectionInfo.getPassword, query)
            try {
              checkExistsDataWithCheckMiss(records.rs, listDateTarget, prerequisiteJobNameStr, emptyFlag, values)
            }
            finally {
              records.close()
            }
          } else {
            val records = checkBusinessColumn(frequencyCheck, businessColumn, prerequisiteSchema, prerequisiteTableCleaned, targetDateCondition,sparkSession)
            if (records.isEmpty) {
              println("Table is no record.")
              (false, Map(prerequisiteJobNameStr -> listDateTarget))
            } else {
              val logRequire = 24 / values.map(_.toInt).getOrElse(1)
              val listLogDate = records.map(_.getString(0)).toList
              val findMiss = listDateTarget.toSet -- listLogDate.toSet
              val numberOfMiss = 24 - logRequire
              if (findMiss.size <= numberOfMiss) {
                println("Table is ready.")
                (true, Map(prerequisiteJobNameStr -> List()))
              } else {
                println("Table is not ready.")
                (false, Map(prerequisiteJobNameStr -> findMiss.toList))
              }
            }
          }

        case _ =>
          val errMsg = s"This frequency $frequencyCheck is not supported"
          throw new InvalidArgumentException(errMsg)
      }
    } catch {
      case e: Exception =>
        throw new InvalidArgumentException(e.getMessage,e)
    }
  }

  override def checkDependencyByJobName(controlJobDf: Row,
                                        masterRefDate: LocalDateTime,postgresConnectionInfo: ConnectionInfo,
                                        refDateIctrlDt: String, startICtrlDt: String,
                                        connectionInfo: ConnectionInfo,jobName:String,
                                        schemaMap: mutable.Map[String,String]): java.util.Map[String,Boolean] = {
    var df = ConnectionService.postgresqlQueryDirectly(
      postgresConnectionInfo.getIp,postgresConnectionInfo.getPort,
      postgresConnectionInfo.getDbName,postgresConnectionInfo.getUserNm,postgresConnectionInfo.getPassword
      ,s"select * from $schemaName.tbl_job_dependency where " +
      s"job_nm = '$jobName' and UPPER(active_flag) = 'Y'")
    val returnJobMap = new java.util.HashMap[String,Boolean]()
    val processResult = processResultSetWithSingleLoop(df.rs)
    df.close()
    if(processResult._1) {
      val r = processResult._3.get
      var preReqSchemaNm = r.prerequisiteSchemaNm
      val map = checkLogIngestion(processResult._2, preReqSchemaNm,
        r.prerequisiteTableNm, r.frequencyCheck,
        Some(r.value), r.emptyFlag, r.dataColumn,
        convertPythonDateFormatToJava(r.ictrlDtTgtfmt),
        convertPythonDateFormatToJava(controlJobDf.getAs[String]("ictrl_dt_tgtfmt")),
        masterRefDate, "tbl_ingest_logs", connectionInfo.getIp, connectionInfo.getPort,
        connectionInfo.getUserNm, connectionInfo.getPassword, connectionInfo.getSid,
        postgresConnectionInfo)
      returnJobMap.put(r.prerequisiteJobNm, map._1)
    }
    else {
      df = ConnectionService.postgresqlQueryDirectly(
        postgresConnectionInfo.getIp, postgresConnectionInfo.getPort,
        postgresConnectionInfo.getDbName, postgresConnectionInfo.getUserNm, postgresConnectionInfo.getPassword
        , s"select * from $schemaName.tbl_job_dependency where " +
          s"job_nm = '$jobName' and UPPER(active_flag) = 'Y'")
      try {
        while (df.rs.next()) {
          val r = df.rs
          val preReqSchemaNm = r.getString("prerequisite_schema_nm")
          val map = checkLogIngestion(r.getString("prerequisite_job_nm"), preReqSchemaNm,
            r.getString("prerequisite_table_nm"), r.getString("frequency_check"),
            Some(r.getString("value")), r.getInt("empty_flag"), r.getString("data_column"),
            convertPythonDateFormatToJava(r.getString("ictrl_dt_tgtfmt")),
            convertPythonDateFormatToJava(controlJobDf.getAs[String]("ictrl_dt_tgtfmt")),
            masterRefDate, "tbl_ingest_logs", connectionInfo.getIp,
            connectionInfo.getPort, connectionInfo.getUserNm, connectionInfo.getPassword,
            connectionInfo.getSid, postgresConnectionInfo)
          returnJobMap.put(r.getString("prerequisite_job_nm"), map._1)
        }
      }
      finally {
        df.close()
      }
    }
    returnJobMap
  }
}
