package com.gable.templar.zeus.custom

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.gable.templar.constant.JobConstant
import com.gable.templar.constant.JobConstant.CATCHUP_TYPE
import com.gable.templar.custom.view.{DependencyCheckModel, ExecuteResponse}
import com.gable.templar.exception.{DropDuplicatesJobError, RunNotebookParallelException}
import com.gable.templar.heaven.exception.InvalidArgumentException
import com.gable.templar.zeus.controller.model.LoginUser
import com.gable.templar.zeus.service.vector.ConnectionInfo
import org.apache.spark.sql.functions.{col, lower}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory
import org.springframework.core.task.TaskExecutor

import java.sql.Timestamp
import scala.collection.JavaConversions._
import java.text.SimpleDateFormat
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.concurrent.{Callable, CompletableFuture, Future}
import java.util.function.Supplier
import java.{lang, util}
import javax.servlet.http.HttpServletRequest
import scala.util.{Failure, Success, Try}
import scala.util.control.Breaks.{break, breakable}
import scala.util.matching.Regex

class IngestFw(override val schemaName: String,
               override val heraUrl: String,
               override val loginUser: LoginUser,
               override val sparkSession: SparkSession,
               override val taskExecutor: TaskExecutor) extends CustomFw {

  private val logger = LoggerFactory.getLogger(classOf[TransformFw])

  def checkRunningIctrlDtIngest(
                                 spark: SparkSession, jobNmUpdate: String, tasksgroupNmUpdate: String,
                                 ictrlDtUpdate: String, roundTime: LocalDateTime, jobStartTimeUpdate: LocalDateTime,
                                 startIctrlDtStrUpdate: String, endIctrlDtStrUpdate: String, processJobType: String = "manual",
                                 cdrFlag: Boolean = false, dagRunId: String, appIdUpdate: String,
                                 connectionInfo: ConnectionInfo,sparkSession: SparkSession): Unit = {


    println(s"Running time to check log: ${LocalDateTime.now()}")
    println(s"Round_time On Checking: $roundTime")

    val hoursCheckRoundTime = 3
    val ingestAuditLogsTable = s"$schemaName.tbl_ingest_audit_logs"
    val queryIngLog = if (!cdrFlag) {
      s"""
    SELECT job_nm, tasksgroup_nm, round_time, dag_run_id, target_schema_nm, target_table_nm, job_start_time, job_end_time, ictrl_dt, status
    FROM $ingestAuditLogsTable
    WHERE lower(job_nm) = lower($jobNmUpdate) AND lower(tasksgroup_nm) = lower($tasksgroupNmUpdate) AND ictrl_dt = $ictrlDtUpdate AND status = 'RUNNING'
    ORDER BY round_time DESC
    LIMIT 1
    """
    } else {
      println(s"CDR FLAG IS $cdrFlag")
      s"""
    SELECT job_nm, tasksgroup_nm, round_time, dag_run_id, target_schema_nm, target_table_nm, job_start_time, job_end_time, ictrl_dt, status
    FROM $ingestAuditLogsTable
    WHERE lower(job_nm) = lower($jobNmUpdate) AND lower(tasksgroup_nm) = lower($tasksgroupNmUpdate) AND ictrl_dt IS NOT NULL AND status = 'RUNNING'
    ORDER BY round_time DESC
    LIMIT 1
    """
    }

    val dfResultLog = ConnectionService.postgresqlQueryFunc(
      connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
      connectionInfo.getUserNm, connectionInfo.getPassword, queryIngLog,sparkSession)

    val resultLog = dfResultLog.collect()

    // Has Log Running
    if (resultLog.length > 0) {
      val valueLog = resultLog.head
      val lastRoundTimeWIctrlDt = valueLog.getAs[Timestamp]("round_time")

      val strFormatRoundTime = new java.text.SimpleDateFormat("yyyyMMdd").format(lastRoundTimeWIctrlDt)
      val strRoundTimeIn = new java.text.SimpleDateFormat("yyyyMMdd").format(roundTime)

      if (strFormatRoundTime != strRoundTimeIn) {
        println("Check log Complete, current round_time is newer by 1 day than the last round_time.")
      } else {
        val errMsg = s"Found Status RUNNING Job on ictrl_dt: $ictrlDtUpdate Running ON -> dag_run_id: ${valueLog.getAs[String]("dag_run_id")} round_time: ${valueLog.getAs[Timestamp]("round_time")}"
        val errMsgUpdate = s"DropDuplicatesJobError: $errMsg"
        println("-- Has Log Running in ingest audit log --")
        dfResultLog.show()
        println("Update Log Function")
        val queryUpdateLog =
          s"""
        UPDATE $ingestAuditLogsTable
        SET job_start_time = $jobStartTimeUpdate,
        ictrl_dt = $ictrlDtUpdate,
        start_ictrl_dt = $startIctrlDtStrUpdate,
        end_ictrl_dt = $endIctrlDtStrUpdate,
        status = 'FAILED',
        job_end_time = $jobStartTimeUpdate,
        duration = '00:00:00',
        source_cnt = 0,
        process_cnt = 0,
        row_cnt = 0,
        err_msg = '$errMsgUpdate',
        app_id = $appIdUpdate
        WHERE job_nm = $jobNmUpdate AND tasksgroup_nm = $tasksgroupNmUpdate AND round_time = $roundTime AND dag_run_id = $dagRunId
        """
        ConnectionService.postgresqlInsertUpdateFunc(
          connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
          connectionInfo.getUserNm, connectionInfo.getPassword, queryUpdateLog,Seq.empty
        )
        println("Update audit log Complete")
        throw new DropDuplicatesJobError(errMsg)
      }
    }

    // Checking Running Ictrl_dt With Null
    val checkRoundTime = roundTime.minusHours(hoursCheckRoundTime)
    val strCheckRoundTime = checkRoundTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val strCheckCurrectRoundTime = roundTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    val queryIngLogNullIctrlDt =
      s"""
  SELECT job_nm, tasksgroup_nm, round_time, dag_run_id, target_schema_nm, target_table_nm, job_start_time, job_end_time, ictrl_dt, status
  FROM $ingestAuditLogsTable
  WHERE lower(job_nm) = lower($jobNmUpdate) AND lower(tasksgroup_nm) = lower($tasksgroupNmUpdate) AND ictrl_dt IS NULL AND status = 'RUNNING' AND round_time >= '$strCheckRoundTime' AND round_time < '$strCheckCurrectRoundTime'
  ORDER BY round_time DESC
  LIMIT 1
  """

    val dfResultLogNull = ConnectionService.postgresqlQueryFunc(
      connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
      connectionInfo.getUserNm, connectionInfo.getPassword, queryIngLogNullIctrlDt,spark)

    val resultLogNull = dfResultLogNull.collect()

    if (resultLogNull.length > 0) {
      val valueLog = resultLogNull.head
      val errMsg = s"Found Status RUNNING round_time under $hoursCheckRoundTime with ictrl_dt NULL : Running ON -> dag_run_id: ${valueLog.getAs[String]("dag_run_id")} round_time: ${valueLog.getAs[Timestamp]("round_time")}"
      val errMsgUpdate = s"DropDuplicatesJobError: $errMsg"
      val queryUpdateLog =
        s"""
      UPDATE $ingestAuditLogsTable
      SET job_start_time = $jobStartTimeUpdate,
      ictrl_dt = $ictrlDtUpdate,
      start_ictrl_dt = $startIctrlDtStrUpdate,
      end_ictrl_dt = $endIctrlDtStrUpdate,
      status = 'FAILED',
      job_end_time = $jobStartTimeUpdate,
      duration = '00:00:00',
      source_cnt = 0,
      process_cnt = 0,
      row_cnt = 0,
      err_msg = '$errMsgUpdate',
      app_id = $appIdUpdate
      WHERE job_nm = $jobNmUpdate AND tasksgroup_nm = $tasksgroupNmUpdate AND round_time = $roundTime AND dag_run_id = $dagRunId
      """
      ConnectionService.postgresqlInsertUpdateFunc(
        connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
        connectionInfo.getUserNm, connectionInfo.getPassword, queryUpdateLog)
      println("Update audit log Complete")
      throw new DropDuplicatesJobError(errMsg)
    }

    // More Codition Check SUCCEED Log
    if (processJobType == "ongoing" && cdrFlag) {
      val queryIngLogSuccess =
        s"""
    SELECT job_nm, tasksgroup_nm, round_time, dag_run_id, target_schema_nm, target_table_nm, job_start_time, job_end_time, ictrl_dt, status
    FROM $ingestAuditLogsTable
    WHERE lower(job_nm) = lower($jobNmUpdate) AND lower(tasksgroupNm) = lower($tasksgroupNmUpdate) AND ictrl_dt = $ictrlDtUpdate AND status = 'SUCCEED' AND err_msg = '-'
    ORDER BY round_time DESC
    LIMIT 1
    """
      val dfResultLogSuccess = ConnectionService.postgresqlQueryFunc(
        connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
        connectionInfo.getUserNm, connectionInfo.getPassword, queryIngLogSuccess,spark)

      val resultLogSuccess = dfResultLogSuccess.collect()

      if (resultLogSuccess.length > 0) {
        val valueLog = resultLogSuccess.head
        val errMsg = s"Found Status SUCCEED Job on ictrl_dt: $ictrlDtUpdate Running ON -> dag_run_id: ${valueLog.getAs[String]("dag_run_id")} round_time: ${valueLog.getAs[Timestamp]("round_time")}"
        println(errMsg)

        println("-- Has Log Success in ingest audit log --")
        dfResultLogSuccess.show()

        val queryDeleteLog =
          s"""
      DELETE FROM $ingestAuditLogsTable
      WHERE job_nm = $jobNmUpdate AND tasksgroup_nm = $tasksgroupNmUpdate AND round_time = $roundTime AND dag_run_id = $dagRunId
      """
        ConnectionService.postgresqlInsertUpdateFunc(
          connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
          connectionInfo.getUserNm, connectionInfo.getPassword, queryDeleteLog
        )
        println("Delete audit log Complete")
        throw new DropDuplicatesJobError(errMsg)
      } else {
        println("Check log Complete, Has No job SUCCEED with ictrl_dt")
      }
    }
  }

  def doGetDelayDayStatus(postgresConnectionInfo: ConnectionInfo,
                          currentLocalDateRun: LocalDateTime, jobName: String,
                          refDateIctrlDt: String, dateFormatIctrlDtForTb: String): String = {
    val queryIngestionLogs = f"select * from $schemaName.tbl_ingest_audit_logs where lower(job_nm) = lower('${jobName}')"
    val ingestionLogJobs = ConnectionService.postgresqlQueryFunc(postgresConnectionInfo.getIp, postgresConnectionInfo.getPort,
      postgresConnectionInfo.getDbName, postgresConnectionInfo.getUserNm, postgresConnectionInfo.getPassword, queryIngestionLogs,sparkSession)
    val currentLocalDate = currentLocalDateRun.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val getLogSuccessPostgresql =  f"""SELECT job_nm, job_start_time, status
        FROM (
            SELECT
                job_nm,
                status,
                job_start_time,
                ROW_NUMBER() OVER (PARTITION BY job_start_time ORDER BY job_start_time DESC) AS rn
            FROM fwconfz.tbl_ingest_audit_logs
            WHERE
                job_start_time BETWEEN
                    (TO_TIMESTAMP('$currentLocalDate 00:00:00', 'YYYY-MM-DD HH24:MI:SS') - INTERVAL '1 day')
                    AND TO_TIMESTAMP('$currentLocalDate 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
                AND status = 'SUCCEED'
                AND job_nm = 'check_job_nm'
        ) a
        WHERE rn = 1
        ORDER BY job_start_time DESC
        LIMIT 1"""
    val ingestionLogsSuccess = ConnectionService.postgresqlQueryFunc(
      postgresConnectionInfo.getIp, postgresConnectionInfo.getPort,
      postgresConnectionInfo.getDbName, postgresConnectionInfo.getUserNm,
      postgresConnectionInfo.getPassword,
      getLogSuccessPostgresql,sparkSession)
    val delayTimeResult = delayTime(sparkSession, jobName,refDateIctrlDt,
      currentLocalDate,null, dateFormatIctrlDtForTb)
    val logSuccessCount = ingestionLogsSuccess.count()
    var delayDayStatus = "-"
    if(delayTimeResult._1.isEmpty && delayTimeResult._2 == "-") {

    }
    else if(delayTimeResult._1.head == "Delay flag is off"
      && delayTimeResult._2 == "Delay flag is off") {

    }
    else if(ingestionLogJobs.isEmpty) {
      if(!delayTimeResult._1.contains(currentLocalDate)) {
        delayDayStatus = "Not a Date to do this job"
      }
    }
    else if(logSuccessCount == 1 && delayTimeResult._1.contains(currentLocalDate)) {
      delayDayStatus = "Job Has already Done"
    }
    else if(!delayTimeResult._1.contains(currentLocalDate)) {
      delayDayStatus = "Not a Date to do this job"
    }
    delayDayStatus
  }

  override def doRunFramework(dependencyCheckModel: DependencyCheckModel,
                              JOB_TYPE: JobConstant.JOB_TYPE,jobName: String,
                              controlJobDf: Row,tblConfName: String,
                              httpServletRequest: HttpServletRequest,
                              username: String): CompletableFuture[ExecuteResponse] = {
    val completableFuture: CompletableFuture[ExecuteResponse] = CompletableFuture.supplyAsync(new Supplier[ExecuteResponse] {
      override def get(): ExecuteResponse =  {
        {
          if (dependencyCheckModel.get_bldStartDate() == null) {
            dependencyCheckModel.set_bldStartDate(new Timestamp(System.currentTimeMillis()))
          }
          val roundTime = LocalDateTime.now()
          if (dependencyCheckModel.getModuleNotebookName == null)
            dependencyCheckModel.setModuleNotebookName("zeppelin-se-uat-g")
          var masterRefDate: LocalDateTime = null
          val frequency = controlJobDf.getAs[String]("frequency")
          val backdate = controlJobDf.getAs[Any]("back_day")
          var currentLocalDateRun: LocalDateTime = null
          val catchUpType = controlJobDf.getAs[String]("catchup_type")
          val ictrlDtTgtFmt = controlJobDf.getAs[String]("ictrl_dt_tgtfmt")
          val dateFormatIctrlDtForTb = convertPythonDateFormatToJava(ictrlDtTgtFmt)
          val timeRetry = controlJobDf.getAs[Int]("time_retry")
          val totalRetry = controlJobDf.getAs[Int]("total_retry")
          var currentDateRun = controlJobDf.getAs[Any]("last_success_ictrl_dt").toString
          var origRefDate: LocalDateTime = null
          if (dependencyCheckModel.getFixedDate == null || dependencyCheckModel.getFixedDate.isEmpty) {
            masterRefDate = minusDateByFrequency(dependencyCheckModel.get_bldStartDate().toLocalDateTime, frequency, backdate)
            origRefDate = dependencyCheckModel.get_bldStartDate().toLocalDateTime
          }
          else {
            masterRefDate = parseToLocalDateTime(dependencyCheckModel.getFixedDate, dateFormatIctrlDtForTb).get
            origRefDate = dependencyCheckModel.get_bldStartDate().toLocalDateTime
          }
          if (currentDateRun == null) {
            currentLocalDateRun = masterRefDate
            currentDateRun = masterRefDate.format(DateTimeFormatter.ofPattern(dateFormatIctrlDtForTb))
          }
          else {
            currentLocalDateRun = parseToLocalDateTime(currentDateRun, dateFormatIctrlDtForTb).get
            if (catchUpType.equals(CATCHUP_TYPE.SEQUENCE.getValue)) {
              currentLocalDateRun = addDateByFrequency(currentLocalDateRun, frequency)
            }
          }
          var isCdr: Boolean = false
          if (JOB_TYPE == JobConstant.JOB_TYPE.FILE) {
            isCdr = controlJobDf.getAs[String]("cdr_flag").equals("Y")
          }
          if (isCdr) {
            currentLocalDateRun = masterRefDate
          }
          val startIctrlDt = currentLocalDateRun.format(DateTimeFormatter.ofPattern(dateFormatIctrlDtForTb))
          val endIctrlDt = masterRefDate.format(DateTimeFormatter.ofPattern(dateFormatIctrlDtForTb))
          val sb = new StringBuilder()
          var overlap: String = "0"
          if (JOB_TYPE != JobConstant.JOB_TYPE.KAFKA) {
            overlap = controlJobDf.getAs[String]("overlap")
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
                runId = dependencyCheckModel.get_workflowId() + "||" + dependencyCheckModel.get_runId() + "||" + dependencyCheckModel.get_taskId()
              }
              else {
                runId = "manual_run_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HH_mm_ss"))
              }
              ictrlDtRun = masterRefDate
              if (catchUpType.equalsIgnoreCase(CATCHUP_TYPE.SEQUENCE.getValue)) {
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
              val startDateWithOverlap = calOverLap(ictrlDtRun, overlap, frequency)
              val startDateWithOverlapIctrlDt = startDateWithOverlap.format(DateTimeFormatter.ofPattern(dateFormatIctrlDtForTb))
              val refDateIctrlDt = ictrlDtRun.format(DateTimeFormatter.ofPattern(dateFormatIctrlDtForTb))
              val runTime: LocalDateTime = LocalDateTime.now()
              insertRoundAuditLog(dependencyCheckModel,
                controlJobDf.getAs[String]("schema_nm"), controlJobDf.getAs[String]("table_nm"),
                runId, refDateIctrlDt, postgresConnectionInfo, runTime, startIctrlDt, endIctrlDt,
                roundTime,controlJobDf.getAs[String]("load_type"),controlJobDf.getAs[String]("ingest_type"),
                controlJobDf.getAs[String]("tasksgroup_nm"))
              checkRunningIctrlDtIngest(sparkSession, jobName,
                dependencyCheckModel.getTaskGroupName, refDateIctrlDt, roundTime, runTime, startIctrlDt,
                endIctrlDt, "ongoing", isCdr, runId, sparkSession.sparkContext.applicationId, postgresConnectionInfo,
                sparkSession)
              try {
                breakable {
                  for (i <- 0 to totalRetry) {
                    val queryMasterSourceSql = s"SELECT system,key,values FROM $schemaName.tbl_master_config where system = '${controlJobDf.getAs[String]("connector_source")}'"
                    val connectionInfo = ConnectionService.getMasterConfigLog(queryMasterSourceSql, salt, ultKey)
                    val results: java.util.Map[String, Boolean] =
                      checkDependencyByJobName(controlJobDf, ictrlDtRun, postgresConnectionInfo,
                        refDateIctrlDt, currentDateRun, connectionInfo, jobName)
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
                    Thread.sleep(timeRetry * 1000)
                  }
                }
                val param: java.util.HashMap[String, JsonNode] = new util.HashMap[String, JsonNode]()
                param.put("job_nm", objectMapper.valueToTree(jobName))
                param.put("tasksgroup_nm", objectMapper.valueToTree(dependencyCheckModel.getTaskGroupName))
                param.put("rl_ref_date", objectMapper.valueToTree(origRefDate))
                param.put("ictrl_dt", objectMapper.valueToTree(ictrlDtRun))
                param.put("start_ictrl_dt", objectMapper.valueToTree(startIctrlDt))
                param.put("end_ictrl_dt", objectMapper.valueToTree(endIctrlDt))
                param.put("start_ictrl_dt_w_overlap", objectMapper.valueToTree(startDateWithOverlapIctrlDt))
                param.put("end_ictrl_dt_w_overlap", objectMapper.valueToTree(endIctrlDt))
                param.put("ictrl_dt_type", objectMapper.valueToTree(controlJobDf.getAs[String]("ictrl_dt_type")))
                param.put("ictrl_dt_tgtfmt", objectMapper.valueToTree(ictrlDtTgtFmt))
                param.put("frequency_job", objectMapper.valueToTree(frequency))
                param.put("load_type", objectMapper.valueToTree(controlJobDf.getAs[String]("load_type")))
                param.put("table_conf", objectMapper.valueToTree(tblConfName))
                param.put("manual_ref_date", objectMapper.valueToTree(false))
                param.put("dag_run_id", objectMapper.valueToTree(runId))
                if (dependencyCheckModel.getTaskGroupName != null)
                  param.put("job_run_mode", objectMapper.valueToTree("tasksgroup"))
                else
                  param.put("job_run_mode", objectMapper.valueToTree("job"))
                param.put("round_time", objectMapper.valueToTree(roundTime))
                val runNotebookParallelResult =
                  doRunNotebookParallel(param, "", dependencyCheckModel,
                    username, runId,httpServletRequest)
                var status = ""
                if (runNotebookParallelResult.getErrorMsg != null) {
                  status = "FAILED"
                  postProcess(status, dependencyCheckModel,
                    runNotebookParallelResult.getErrorSpecificMsg,
                    runId, sparkSession,
                    runNotebookParallelResult.getNotebookUrl, runTime,
                    LocalDateTime.now(), roundTime, postgresConnectionInfo, refDateIctrlDt, jobName,
                    "tbl_trans_audit_logs", tblConfName)
                  throw new RunNotebookParallelException(runNotebookParallelResult.getErrorMsg)
                }
                postProcess(status, dependencyCheckModel, runNotebookParallelResult.getErrorSpecificMsg,
                  runId, sparkSession, runNotebookParallelResult.getNotebookUrl, runTime,
                  LocalDateTime.now(), roundTime, postgresConnectionInfo, refDateIctrlDt, jobName,
                  "tbl_trans_audit_logs", tblConfName)
                sb.append(runNotebookParallelResult.getMessage)
              }
              catch {
                case exception: Exception => {
                  logger.error(exception.getMessage,exception)
                  if(!exception.getClass.equals(classOf[RunNotebookParallelException])) {
                    updateStateOfAuditLogByJobNameAndRoundTimeAndDagRun(
                      "FAILED",
                      dependencyCheckModel,runId,LocalDateTime.now(),
                      roundTime,postgresConnectionInfo,exception.getMessage,"tbl_trans_audit_logs")
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
                          taskGroupName:String): Unit = {
    val seqValue = Seq(dependencyCheckModel.getJobName,roundTime,runId,
      schemaName,tableName,jobStartTime,ictrlDt,startIctrlDt,endIctrlDt,
      dependencyCheckModel.getModuleNotebookName,loadType,ingestType,taskGroupName)
    val sql = f"insert into ${this.schemaName}.tbl_ingest_audit_logs (job_nm,round_time," +
      f"dag_run_id,target_schema_nm,target_table_nm,ingestion_type," +
      f"load_type,job_start_time,ictrl_dt,start_ictrl_dt," +
      f"end_ictrl_dt,status,zeppelin,load_type,ingest_type,tasksgroup_nm) values (" +
      f"?,?,?,?,?,?,?" +
      f"?,?,?,?,'RUNNING'," +
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

  def checkLogIngestion(prerequisiteJobNm: Any, prerequisiteSchema: String, prerequisiteTable: String,
                        frequencyCheck: String, values: Option[String], emptyFlag: Int,
                        businessColumn: String, patternIctrlDtCheck: String,
                        patternIctrlDt: String = "", masterRefDate: LocalDateTime,
                        logTable: String = "tbl_ingest_logs", depenIp: String = "", depenPort: String = "",
                        depenUserNm: String = "", depenPassword: String = "", depenSid: String = "",
                        postgresConnectionInfo:ConnectionInfo): (Boolean, Map[String, List[String]]) = {

    def convertPythonToPysparkFormat(pyFormat: String): String = {
      val pythonToPyspark = Map(
        "%Y" -> "yyyy", "%y" -> "yy", "%m" -> "MM", "%B" -> "MMMM", "%b" -> "MMM",
        "%d" -> "dd", "%H" -> "HH", "%I" -> "hh", "%M" -> "mm", "%S" -> "ss",
        "%f" -> "SSS", "%a" -> "EEE", "%A" -> "EEEE", "%w" -> "e", "%p" -> "a",
        "%z" -> "Z", "%Z" -> "z"
      )
      pythonToPyspark.foldLeft(pyFormat)((current, entry) => current.replace(entry._1, entry._2))
    }

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

      val baseQuery = if (logTable == "tbl_ingest_logs") {
        s"select target_table_nm, job_start_time, ictrl_dt, status, row_cnt from $tblIngestAuditLogs " +
          s"where upper(job_nm) = upper('$prerequisiteJobNameStr') and target_schema_nm = '$prerequisiteSchema' and target_table_nm = '$prerequisiteTableCleaned'"
      } else {
        s"select table_nm, job_start_time, ictrl_dt, status, row_cnt from $tblTauditLogs " +
          s"where upper(job_nm) = upper('$prerequisiteJobNameStr') and schema_nm = '$prerequisiteSchema' and table_nm = '$prerequisiteTableCleaned'"
      }

      frequencyCheck match {
        case "daily" =>
          val targetDate = masterRefDate.format(DateTimeFormatter.ofPattern(patternIctrlDtCheck))
          val query = s"$baseQuery and ictrl_dt like '$targetDate%' order by job_start_time desc"

          val records = if (checkInDate == "logs" && prerequisiteJobNameStr.nonEmpty) {
            ConnectionService.postgresqlQueryFunc(postgresConnectionInfo.getIp, postgresConnectionInfo.getPort, postgresConnectionInfo.getDbName, postgresConnectionInfo.getUserNm, postgresConnectionInfo.getPassword,query,sparkSession).collect()
          } else if (checkInDate == "rawDate") {
            checkBusinessColumn(frequencyCheck, businessColumn, prerequisiteSchema, prerequisiteTableCleaned, targetDate,sparkSession)
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
            oracleIngestionQueryLog(sparkSession,depenIp, depenPort, depenUserNm, depenPassword, dbDailyQuery, depenSid).collect()
          }

          if (records.isEmpty) {
            println("Table is no record.")
            val name = if (prerequisiteJobNameStr.nonEmpty) prerequisiteJobNameStr else prerequisiteTableCleaned
            (false, Map(name -> List(targetDate)))
          } else if (checkInDate == "rawDate") {
            println("table is record.")
            (true, Map(prerequisiteJobNameStr -> List()))
          } else {
            val status = records.head.getString(3)
            val rowCnt = if (records.head.get(4) != null) records.head.getLong(4) else 0L
            if (status == "SUCCEED" && rowCnt >= emptyFlag) {
              println("table is ready.")
              (true, Map(prerequisiteJobNameStr -> List()))
            } else {
              println("table is not ready.")
              (false, Map(prerequisiteJobNameStr -> List(targetDate)))
            }
          }

        case "hourly" =>
          val hoursOffset = values.map(_.toInt).getOrElse(0)
          val targetDateTime = masterRefDate.minus(hoursOffset, ChronoUnit.HOURS)
          val targetDate = targetDateTime.format(DateTimeFormatter.ofPattern(patternIctrlDtCheck))
          val query = s"$baseQuery and ictrl_dt like '$targetDate%' order by job_start_time desc"
          println(s"Query: $query")

          val records = if (checkInDate == "logs") {
            ConnectionService.postgresqlQueryFunc(postgresConnectionInfo.getIp, postgresConnectionInfo.getPort,
              postgresConnectionInfo.getDbName, postgresConnectionInfo.getUserNm,
              postgresConnectionInfo.getPassword, query,sparkSession).collect()
          } else {
            checkBusinessColumn(frequencyCheck, businessColumn, prerequisiteSchema, prerequisiteTableCleaned, targetDate,sparkSession)
          }

          if (records.isEmpty) {
            println("Table is no record.")
            (false, Map(prerequisiteJobNameStr -> List(targetDate)))
          } else if (checkInDate == "rawDate") {
            println("table is record.")
            (true, Map(prerequisiteJobNameStr -> List()))
          } else {
            val status = records.head.getString(3)
            val rowCnt = if (records.head.get(4) != null) records.head.getLong(4) else 0L
            if (status == "SUCCEED" && rowCnt >= emptyFlag) {
              println("table is ready.")
              (true, Map(prerequisiteJobNameStr -> List()))
            } else {
              println("table is not ready.")
              (false, Map(prerequisiteJobNameStr -> List(targetDate)))
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

          val records = if (checkInDate == "logs") {
            ConnectionService.postgresqlQueryFunc(postgresConnectionInfo.getIp, postgresConnectionInfo.getPort,
              postgresConnectionInfo.getDbName, postgresConnectionInfo.getUserNm,
              postgresConnectionInfo.getPassword, query,sparkSession).collect()
          } else {
            checkBusinessColumn(frequencyCheck, businessColumn, prerequisiteSchema, prerequisiteTableCleaned, targetDateCondition,sparkSession)
          }

          if (records.isEmpty) {
            println("Table is no record.")
            (false, Map(prerequisiteJobNameStr -> listDateTarget))
          } else if (checkInDate == "rawDate") {
            val listLogDate = records.map(_.getString(0)).toList
            val findMiss = listDateTarget.toSet -- listLogDate.toSet
            if (findMiss.isEmpty) {
              println("Table is ready.")
              (true, Map(prerequisiteJobNameStr -> List()))
            } else {
              println("Table is not ready.")
              (false, Map(prerequisiteJobNameStr -> findMiss.toList))
            }
          } else { // checkInDate is "logs"
            val rowCnt = if (records.head.get(4) != null) records.head.getLong(4) else 0L
            val listLogDate = records.filter(row =>
              row.getString(3) == "SUCCEED" && row.get(4) != null && row.getLong(4) >= emptyFlag
            ).map(_.getString(2)).toList

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

          val records = if (checkInDate == "logs") {
            ConnectionService.postgresqlQueryFunc(postgresConnectionInfo.getIp, postgresConnectionInfo.getPort,
              postgresConnectionInfo.getDbName, postgresConnectionInfo.getUserNm,
              postgresConnectionInfo.getPassword, query,sparkSession).collect()
          } else {
            checkBusinessColumn(frequencyCheck, businessColumn, prerequisiteSchema, prerequisiteTableCleaned, targetDateCondition,sparkSession)
          }

          if (records.isEmpty) {
            println("Table is no record.")
            (false, Map(prerequisiteJobNameStr -> listDateTarget))
          } else if (checkInDate == "rawDate") {
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
          } else { // checkInDate is "logs"
            val logRequire = 24 / values.map(_.toInt).getOrElse(1)
            val listLogDate = records.filter(row =>
              row.getString(3) == "SUCCEED" && row.get(4) != null && row.getLong(4) >= emptyFlag
            ).map(_.getString(2)).toList

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
                                        masterRefDate: LocalDateTime,connectionInfo: ConnectionInfo,
                                        refDateIctrlDt: String, startICtrlDt: String,
                                        postgresConnectionInfo: ConnectionInfo,jobName:String): java.util.Map[String,Boolean] = {
    val df = sparkSession.sql(s"select * from $schemaName.tbl_job_dependency where " +
      s"job_nm = '$jobName' and UPPER(active_flag) = 'Y'")
    val returnJobMap = new java.util.HashMap[String,Boolean]()
    df.collect().foreach(r => {
      var preReqSchemaNm = r.getAs[String]("prerequisite_schema_nm")
      if(!schemaName.endsWith("_uat")) {
        preReqSchemaNm = preReqSchemaNm.replace("_uat","")
      }
      val map = checkLogIngestion(r.getAs[String]("prerequisite_job_nm"),preReqSchemaNm,
        r.getAs[String]("prerequisite_table_nm"),r.getAs[String]("frequency_check"),
        Some(r.getAs[String]("value")),r.getAs[Int]("empty_flag"),r.getAs[String]("data_column"),
        r.getAs[String]("ictrl_dt_tgtfmt"),controlJobDf.getAs[String]("ictrl_dt_tgtfmt"),
        masterRefDate,f"$schemaName.tbl_ingest_audit_logs",connectionInfo.getIp,connectionInfo.getPort,
        connectionInfo.getUserNm,connectionInfo.getPassword,connectionInfo.getSid,
        postgresConnectionInfo)
      returnJobMap.put(r.getAs[String]("prerequisite_job_nm"),map._1)
    })
    returnJobMap
  }
}
