package com.gable.templar.zeus.custom

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.gable.templar.constant.JobConstant
import com.gable.templar.constant.JobConstant.{JOB_TYPE, importParameter, initialTitle, manualTitle}
import com.gable.templar.custom.view.{AirflowModelView, DependencyCheckModel, ExecuteResponse, NotebookCheckParallelRequest, NotebookIdAddParameterRequest, NotebookRunParallelRequest, NotebookRunParallelResponse, RunNotebookParallelResult, SequenceJobInformation}
import com.gable.templar.exception.{DropDuplicatesJobError, DropSuccessJobError}
import com.gable.templar.heaven.exception.InvalidArgumentException
import com.gable.templar.heaven.util.{HTTPServletRequestUtil, RestTemplateFactoryUtil}
import com.gable.templar.zeus.controller.model.LoginUser
import com.gable.templar.zeus.service.vector.ConnectionInfo
import org.apache.spark.sql.types.{BinaryType, BooleanType, DataType, DateType, DecimalType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import org.springframework.core.task.TaskExecutor
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import org.springframework.web.client.HttpServerErrorException
import org.springframework.web.context.request.{RequestContextHolder, ServletRequestAttributes}

import java.sql.{DriverManager, ResultSet, ResultSetMetaData, Timestamp, Types}
import java.time.{Duration, LocalDate, LocalDateTime, YearMonth}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.temporal.ChronoUnit
import java.util
import java.util.concurrent.{CompletableFuture, Future, Semaphore, ThreadPoolExecutor}
import javax.servlet.http.HttpServletRequest
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

trait CustomFw {

  val salt = "rTYlPkZH37QOf7Xx1GzZ0hakdl/2/Z02HlPesDfQ2lM="

  val ultKey = "AdKX67Zn0JRJSGJQ7/4LrQOsZ0IW8+Fcdh7hpeJV8GeVNiPIs4i0RZ4T+XjXyEb0"

  private val logger = LoggerFactory.getLogger(classOf[CustomFw])


  val taskExecutor: TaskExecutor

  val RETRY_COUNT = 3

  val schemaName = ""

  val sparkSession: SparkSession

  val HOURS_CHECK_ROUND_TIME = 3

  val CONCURRENT_SCHEDULE = 4


  var tmpzSchema: String = {
    if (schemaName.endsWith("true_dev")) {
      "tmpz_true_dev"
    }
    else if (schemaName.endsWith("_uat")) {
      "tmpz_uat"
    }
    else {
      "tmpz"
    }
  }

  val heraUrl: String = ""

  val loginUser: LoginUser

  val objectMapper: ObjectMapper = new ObjectMapper()

  val scalaObjectMapper: ObjectMapper = new ObjectMapper()

  scalaObjectMapper.registerModule(DefaultScalaModule)

  def doRunFramework(dependencyCheckModel: DependencyCheckModel,
                     JOB_TYPE: JOB_TYPE, jobName: String, controlJobDf: Row,
                     tblConfName: String, httpServletRequest: HttpServletRequest,
                     username: String,roundTime: LocalDateTime): CompletableFuture[ExecuteResponse]

  def checkDependencyByJobName(controlJobDf: Row,
                               masterRefDate: LocalDateTime, connectionInfo: ConnectionInfo,
                               refDateIctrlDt: String, startICtrlDt: String,
                               postgresConnectionInfo: ConnectionInfo, jobName: String,
                               schemaMap:mutable.Map[String,String]): java.util.Map[String, Boolean]

  def truncateToFormat(ldt: LocalDateTime, format: String): LocalDateTime = {
    format match {
      case "yyyyMM"       => ldt.withDayOfMonth(1).toLocalDate.atStartOfDay()
      case "yyyyMMdd"     => ldt.toLocalDate.atStartOfDay()
      case "yyyyMMddHH"   => ldt.truncatedTo(ChronoUnit.HOURS)
      case "yyyyMMddHHmm" => ldt.truncatedTo(ChronoUnit.MINUTES)
      case "yyyyMMddHHmmss" => ldt.truncatedTo(ChronoUnit.SECONDS)
      case _ => throw new IllegalArgumentException(s"Unsupported format: $format")
    }
  }

  def parseToLocalDateTime(input: String, format: String): Option[LocalDateTime] = {
    try {
      Some(LocalDateTime.parse(input, DateTimeFormatter.ofPattern(format)))
    } catch {
      case _: DateTimeParseException =>
        try {
          try {
            // Try parsing as LocalDate, then convert
            var date: LocalDate = null
            if(input.length < 8) {
              date = YearMonth.parse(input,DateTimeFormatter.ofPattern(format)).atDay(1)
            }
            else {
              date = LocalDate.parse(input, DateTimeFormatter.ofPattern(format))
            }
            Some(date.atStartOfDay())
          } catch {
            case d: DateTimeParseException => {
              logger.error(d.getMessage,d)
              throw new InvalidArgumentException("cannot parse date time")
            }
          }
        }
    }
  }

  def getSequenceAndJob(rows: Array[Row],tblConf: String,jobType: JOB_TYPE): mutable.TreeMap[Int,SequenceJobInformation] = {
    val jobMap: mutable.TreeMap[Int,SequenceJobInformation] = mutable.TreeMap.empty
    rows.foreach(r => {
      var resultSeqInformation = jobMap.get(r.getAs[Int]("sequence")).orNull
      var resultRow: Array[Row] = Array.empty
      if(resultSeqInformation == null) {
        val resultSeqInformationTemp = new SequenceJobInformation
        resultSeqInformationTemp.setData(resultRow)
        resultSeqInformationTemp.setJobType(jobType)
        resultSeqInformationTemp.setTblConf(tblConf)
        resultSeqInformation = resultSeqInformationTemp
      }
      else {
        resultRow = resultSeqInformation.getData
      }
      resultRow = resultRow :+ r
      resultSeqInformation.setData(resultRow)
      jobMap.put(r.getAs[Int]("sequence"),resultSeqInformation)
    })
    jobMap
  }

  def getTblConfNameByJobType(jobTypeList: List[JOB_TYPE]): List[String] = {
    var tblConfList: List[String] = List.empty
    for(jobType <- jobTypeList) {
      jobType match {
        case JOB_TYPE.FILE => {
         tblConfList = tblConfList :+ JobConstant.tableNmFileIngestion
        }
        case JOB_TYPE.KAFKA => {
          tblConfList = tblConfList :+ JobConstant.tableNmKafkaIngestion
        }
        case JOB_TYPE.TRANSFORM => {
          tblConfList = tblConfList :+ JobConstant.tableNmTrans
        }
        case JOB_TYPE.INGEST_DB => {
          tblConfList = tblConfList :+ JobConstant.tableNmDbIngestion
        }
        case JOB_TYPE.INGEST_API => {
          tblConfList = tblConfList :+ JobConstant.tableNmApiIngestion
        }
        case JOB_TYPE.OUTBOUND => {
          tblConfList = tblConfList :+ JobConstant.tableNmOutBound
        }
      }
    }
    tblConfList
  }

  def rsToDataFrame(rs: ResultSet, spark: SparkSession): Array[Row] = {
    val md: ResultSetMetaData = rs.getMetaData
    val n = md.getColumnCount

    def toDataType(i: Int): DataType = md.getColumnType(i) match {
      case Types.BOOLEAN | Types.BIT            => BooleanType
      case Types.TINYINT | Types.SMALLINT       => IntegerType
      case Types.INTEGER                         => IntegerType
      case Types.BIGINT                          => LongType
      case Types.FLOAT | Types.REAL | Types.DOUBLE => DoubleType
      case Types.DECIMAL | Types.NUMERIC =>
        val p = math.max(1, md.getPrecision(i))
        val s = math.max(0, md.getScale(i))
        // Spark DecimalType max precision is 38
        DecimalType(math.min(p, 38), math.min(s, 38))
      case Types.DATE                            => DateType
      case Types.TIME | Types.TIME_WITH_TIMEZONE => StringType // or TimestampType if you convert
      case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE => TimestampType
      case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY => BinaryType
      case _                                     => StringType
    }

    val fields = (1 to n).map { i =>
      // prefer label (AS alias) over raw column name
      val name = Option(md.getColumnLabel(i)).filter(_.nonEmpty).getOrElse(md.getColumnName(i))
      StructField(name, toDataType(i), nullable = true)
    }
    val schema = StructType(fields)
    val useJava8 = spark.conf.getOption("spark.sql.datetime.java8API.enabled").exists(_.toBoolean)

    val rows = ArrayBuffer.empty[Row]
    while (rs.next()) {
      val values = new Array[Any](n)
      var i = 1
      while (i <= n) {
        val v = schema(i-1).dataType match {
          case BooleanType   => { val x = rs.getBoolean(i); if (rs.wasNull()) null else x }
          case IntegerType   => { val x = rs.getInt(i);     if (rs.wasNull()) null else x }
          case LongType      => { val x = rs.getLong(i);    if (rs.wasNull()) null else x }
          case DoubleType    => { val x = rs.getDouble(i);  if (rs.wasNull()) null else x }
          case d: DecimalType=> rs.getBigDecimal(i) // Spark will accept java.math.BigDecimal for DecimalType
          case DateType      =>
            val d = rs.getDate(i)
            if (rs.wasNull()) null
            else if (useJava8) d.toLocalDate else d             // keep java.sql.Date
          case TimestampType =>
            val t = rs.getTimestamp(i)
            if (rs.wasNull()) null
            else if (useJava8) t.toInstant else t    // keep java.sql.Timestamp
          case BinaryType    => rs.getBytes(i)
          case _             => rs.getString(i)            // default to text
        }
        values(i-1) = v
        i += 1
      }
      rows += Row.fromSeq(values)
    }
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    df.collect()
  }


  def doRunTaskGroup(dependencyCheckModel: DependencyCheckModel, jobType: List[JOB_TYPE]): util.ArrayList[ExecuteResponse] = {
    taskExecutor match {
      case tpe: ThreadPoolTaskExecutor =>
        val poolSize = tpe.getPoolSize // Current threads in the pool

        val active = tpe.getActiveCount // Currently running tasks

        logger.info("Active={}",active)
        logger.info("Pool = {}",poolSize)
      case other =>
        logger.info("other class = {}",other.getClass)
    }
    val tblConfName = getTblConfNameByJobType(jobType)
    val queryMasterSql = s"SELECT system,key,values FROM $schemaName.tbl_master_config where system = 'fw_postgre'"
    val postgresConnectionInfo = ConnectionService.getMasterConfigLog(queryMasterSql, salt, ultKey)
    val roundTime = LocalDateTime.now()
    val httpServletRequest = RequestContextHolder.getRequestAttributes.asInstanceOf[ServletRequestAttributes].getRequest
    if (dependencyCheckModel.getTaskGroupName != null) {
      val airflowModelView = RestTemplateFactoryUtil.getRestTemplar(
        HTTPServletRequestUtil.getToken(httpServletRequest),true).getForObject(
        heraUrl + "/private/airflow/getAll/workflow/" + dependencyCheckModel.
          get_workflowId(),classOf[AirflowModelView])
      val taskStartTime = LocalDateTime.now()
      val executeResult = new util.ArrayList[ExecuteResponse]()
      var count = 0
      var mergedTreeMap: mutable.TreeMap[Int, SequenceJobInformation] = mutable.TreeMap.empty[Int,SequenceJobInformation]
      for(tblConf <- tblConfName) {
        logger.info("tbl conf = {}",tblConf)
        val query =
          f"""select *
             |from ${schemaName}.$tblConf
             |where lower(tasksgroup_nm) = lower('${dependencyCheckModel.getTaskGroupName}') and lower(active_flag) = lower('Y')""".stripMargin
        insertTaskgroupLogs(dependencyCheckModel.getTaskGroupName,taskStartTime,roundTime,
          dependencyCheckModel.getModuleNotebookName,postgresConnectionInfo,airflowModelView.getDagName)
        val taskGroupConnection = ConnectionService.postgresqlQueryDirectly(
          postgresConnectionInfo.getIp,postgresConnectionInfo.getPort,
          postgresConnectionInfo.getDbName,postgresConnectionInfo.getUserNm,
          postgresConnectionInfo.getPassword,query)
        try {
          val taskGroupDf = rsToDataFrame(taskGroupConnection.rs, sparkSession)
          mergedTreeMap = mergedTreeMap ++ getSequenceAndJob(taskGroupDf,tblConf,jobType(count))
          count += 1
        }
        finally {
          taskGroupConnection.close()
        }
      }
      val limiter = new Semaphore(CONCURRENT_SCHEDULE, true)
      val errorMsg = new StringBuilder
      var isFailed = false
      logger.info("merged tree map = {}",mergedTreeMap)
      mergedTreeMap.foreach(j => {
        val completableFutureList = ArrayBuffer[CompletableFuture[ExecuteResponse]]()
        j._2.getData.foreach(r => {
          logger.info("job list = {}", r.getAs[String]("job_nm"))
          val executeResponse = new ExecuteResponse
          executeResponse.setJobName(r.getAs[String]("job_nm"))
          limiter.acquire()
          val cf = doRunFramework(dependencyCheckModel,
            j._2.getJobType, r.getAs[String]("job_nm"), r, j._2.getTblConf, httpServletRequest, loginUser.getUsername, roundTime)
          cf.whenComplete((_, _) => limiter.release())
            .whenComplete((res, err) => {
              if (err != null) {
                isFailed = true
                errorMsg.append(err.getMessage)
              }
            })
          completableFutureList += cf
          executeResult.add(executeResponse)
        })
        val allOf = CompletableFuture.allOf(completableFutureList: _*)
        try {
          allOf.join()
        }
        catch {
          case exception: Exception => {
            logger.error(exception.getMessage, exception)
            isFailed = true
          }
        }
      })
      if (isFailed) {
        updateTaskgroupLogs(dependencyCheckModel.getTaskGroupName, taskStartTime, roundTime, "FAILED", postgresConnectionInfo)
        throw new Exception(errorMsg.toString())
      }
      updateTaskgroupLogs(dependencyCheckModel.getTaskGroupName, taskStartTime, roundTime, "SUCCESS", postgresConnectionInfo)
      executeResult
    }
    else {
      val executeResult = new util.ArrayList[ExecuteResponse]()
      val query =
        f"""select * from $schemaName.${tblConfName.head} where lower(job_nm) = lower('${dependencyCheckModel.getJobName}')
           | and lower(active_flag) = lower('Y')
           |""".stripMargin
      val taskGroupConnection = ConnectionService.postgresqlQueryDirectly(
        postgresConnectionInfo.getIp,postgresConnectionInfo.getPort,
        postgresConnectionInfo.getDbName,postgresConnectionInfo.getUserNm,
        postgresConnectionInfo.getPassword,query)
      var row: Row = null
      try{
        row = rsToDataFrame(taskGroupConnection.rs, sparkSession)(0)
      }
      finally{
        taskGroupConnection.close()
      }
      executeResult.add(CompletableFuture.completedFuture(
        doRunFramework(dependencyCheckModel, jobType.head,
          row.getAs[String]("job_nm"), row, tblConfName.head,
          httpServletRequest, loginUser.getUsername,roundTime)).get().get())
      executeResult
    }
  }

  def updateJobStatusAndErrorMessage(jobName: String,roundTime: LocalDateTime, dagRunId: String,
                                     tableNm:String, connectionInfo: ConnectionInfo,status: String,
                                     errorMsg: String,ictrlDt: String): Unit = {
    val params = Seq(status,errorMsg,jobName,dagRunId,roundTime,ictrlDt)
    val sql = f"update ${this.schemaName}.$tableNm set status = ?, err_msg = ? where " +
      f"job_nm = ? and dag_run_id = ? and round_time = ? and ictrl_dt = ?"
    ConnectionService.postgresqlInsertUpdateFunc(connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
      connectionInfo.getUserNm, connectionInfo.getPassword, sql, params)
  }

  def updateJobStartTimeOfAuditLogByJobNameAndRoundTimeAndDagRun(jobName:String, jobStartTime: LocalDateTime,
                                                                 roundTime: LocalDateTime,dagRunId: String,
                                                                 tableNm: String,connectionInfo: ConnectionInfo,
                                                                 ictrlDt: String): Unit = {
    val params = Seq(jobStartTime,jobName,dagRunId,roundTime,ictrlDt)
    val sql = f"update ${this.schemaName}.$tableNm set job_start_time = ? " +
      f"where job_nm = ? and dag_run_id = ? and round_time = ? and ictrl_dt = ?"
    ConnectionService.postgresqlInsertUpdateFunc(connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
      connectionInfo.getUserNm, connectionInfo.getPassword, sql, params)
  }

  def updateStateOfAuditLogByJobNameAndRoundTimeAndDagRun(status: String, dependencyCheckModel: DependencyCheckModel,
                                                          runId: String, jobEndTime: LocalDateTime,
                                                          roundTime: LocalDateTime, connectionInfo: ConnectionInfo,
                                                          errorMsg: String, tableNm: String,jobName: String,ictrlDt:String): Unit = {
    val params = Seq(status, jobEndTime,jobEndTime,"00:00:00",
      jobEndTime, errorMsg, jobName, runId, roundTime,ictrlDt)
    val sql = f"update ${this.schemaName}.$tableNm set status = ?, " +
      f"custom_end_time=?,job_start_time=?,duration=?," +
      f"job_end_time = ?,  err_msg = ? where " +
      f"job_nm = ? and dag_run_id = ? and round_time = ? and ictrl_dt = ?"
    ConnectionService.postgresqlInsertUpdateFunc(connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
      connectionInfo.getUserNm, connectionInfo.getPassword, sql, params)
  }


  def doRunNotebookParallel(param: java.util.HashMap[String, JsonNode], notebookId: String,
                            dependencyCheckModel: DependencyCheckModel, username: String,
                            runId: String, httpServletRequest: HttpServletRequest): RunNotebookParallelResult = {
    val runNotebookParallelResult = new RunNotebookParallelResult
    param.put("dag_run_id", objectMapper.valueToTree(runId))
    val token = HTTPServletRequestUtil.getToken(httpServletRequest)
    val notebookRunParallelRequest = new NotebookRunParallelRequest
    notebookRunParallelRequest.setRunBy(username)
    notebookRunParallelRequest.setAsync(true)
    notebookRunParallelRequest.setRunningId(runId)
    notebookRunParallelRequest.setLanguage("python")
    notebookRunParallelRequest.setModuleNotebookName(dependencyCheckModel.getModuleNotebookName)
    val notebookIdAddParameterRequest: NotebookIdAddParameterRequest = new NotebookIdAddParameterRequest
    val addParameterRequest: java.util.Map[String, java.util.Map[String, JsonNode]] = new java.util.HashMap[String, java.util.Map[String, JsonNode]]()
    addParameterRequest.put(initialTitle, param)
    addParameterRequest.put(manualTitle, param)
    addParameterRequest.put(importParameter,param)
    notebookIdAddParameterRequest.setNotebookId(notebookId)
    notebookIdAddParameterRequest.setAddParameterMapFromTitle(addParameterRequest)
    notebookIdAddParameterRequest.setSparkConf(dependencyCheckModel.getSparkConf)
    val notebookIdAddParameterRequestList: java.util.ArrayList[NotebookIdAddParameterRequest] = new util.ArrayList[NotebookIdAddParameterRequest]()
    notebookIdAddParameterRequestList.add(notebookIdAddParameterRequest)
    notebookRunParallelRequest.setNotebookAddParameterRequest(notebookIdAddParameterRequestList)
    logger.info(f"request  = ${objectMapper.writeValueAsString(notebookRunParallelRequest)}")
    val response = RestTemplateFactoryUtil.getRestTemplar(token, true).postForObject(f"$heraUrl/private/notebook/start/session/parallel", notebookRunParallelRequest, classOf[NotebookRunParallelResponse])
    val notebookList = response.getNotebookRefIds
    logger.info(f"request parallel result = ${objectMapper.writeValueAsString(response)}")
    val notebookCheckParallelRequest = new NotebookCheckParallelRequest
    notebookCheckParallelRequest.setRunningId(runId)
    val returnResponse: StringBuilder = new StringBuilder()
    notebookCheckParallelRequest.setNoteRefIds(notebookList)
    var errorCount = 0
    var notebookUrl = ""
    var timeSleep: Long = 0L
    if (dependencyCheckModel.getTimeSleep == null) {
      timeSleep = 30L
    }
    else {
      timeSleep = dependencyCheckModel.getTimeSleep
      if(timeSleep < 30L) {
        timeSleep = 30L
      }
    }
    var attempt = 0
    while (!notebookList.isEmpty) {
      Thread.sleep(timeSleep * 1000)
      try {
        val response = RestTemplateFactoryUtil.getRestTemplar(token, true).postForObject(f"$heraUrl/private/notebook/session/parallel/check", notebookCheckParallelRequest, classOf[java.util.HashMap[String, java.util.HashMap[String, String]]])
        response.entrySet().forEach(r => {
          logger.info("r value = {}", r.getValue)
          if (!r.getValue.get("status").equals("RUNNING") && !r.getValue.get("status").equals("READY")) {
            returnResponse.append(f"note name = ${r.getKey} run ${r.getValue.get("status")} url = ${r.getValue.get("url")} result = ${r.getValue.get("message")} ${System.lineSeparator()}")
            if (!r.getValue.get("status").equals("SUCCESS")) {
              errorCount += 1
            }
            if (r.getValue.get("url") != null) {
              notebookUrl = r.getValue.get("url")
            }
            notebookList.remove(r.getValue.get("notebookIdRef"))
          }
        })
      }
      catch {
        case e: HttpServerErrorException.BadGateway =>
          logger.error(e.getMessage,e)
          attempt += 1
          if(attempt >= RETRY_COUNT) {
            throw e
          }
      }
    }
    if (errorCount > 0) {
      returnResponse.append("There is error on some notebook")
      runNotebookParallelResult.setErrorMsg(returnResponse.toString())
      runNotebookParallelResult.setErrorSpecificMsg(f"There is error on some notebook url = $notebookUrl")
    }
    runNotebookParallelResult.setNotebookUrl(notebookUrl)
    runNotebookParallelResult.setMessage(returnResponse.toString())
    runNotebookParallelResult
  }

  def calOverLap(dateIn: LocalDateTime, valueOverlap: Any, frequency: String): LocalDateTime = {
    var freq = frequency
    if (freq == null || freq.isEmpty) {
      freq = "daily"
    }
    freq = freq.toLowerCase()

    if (valueOverlap == null || valueOverlap.toString.isEmpty) {
      return dateIn
    }

    val value_overlap_int: Int = valueOverlap match {
      case i: Int => i
      case s: String =>
        if (s.matches("\\d+")) s.toInt
        else {
          println(s"Has Wrong with back_date $valueOverlap")
          throw new Exception("Invalid value_overlap")
        }
      case _ =>
        println(s"Has Wrong with back_date $valueOverlap")
        throw new Exception("Invalid value_overlap")
    }

    if (freq.endsWith("m") && freq != "monthly") {
      val freqParts = freq.split("m")
      val value_in = freqParts(0).trim()
      val value_in_int =
        if (value_in.matches("\\d+")) value_in.toInt
        else {
          println(s"Has Someting Wrong with frequency $freq")
          throw new Exception("Invalid frequency")
        }
      return dateIn.minus(value_in_int.toLong * value_overlap_int, ChronoUnit.MINUTES)
    } else if (freq.endsWith("h")) {
      val freqParts = freq.split("h")
      val value_in = freqParts(0).trim()
      val value_in_int =
        if (value_in.matches("\\d+")) value_in.toInt
        else {
          println(s"Has Someting Wrong with frequency $freq")
          throw new Exception("Invalid frequency")
        }
      return dateIn.minus(value_in_int.toLong * value_overlap_int, ChronoUnit.HOURS)
    } else {
      freq match {
        case "hourly" => dateIn.minus(value_overlap_int.toLong, ChronoUnit.HOURS)
        case "daily" => dateIn.minus(value_overlap_int.toLong, ChronoUnit.DAYS)
        case "weekly" => dateIn.minus(value_overlap_int.toLong * 7, ChronoUnit.DAYS)
        case "monthly" => dateIn.minus(value_overlap_int.toLong, ChronoUnit.MONTHS)
        case "quarterly" => dateIn.minus(value_overlap_int.toLong * 3, ChronoUnit.MONTHS)
        case "yearly" => dateIn.minus(value_overlap_int.toLong, ChronoUnit.YEARS)
        case _ =>
          val errMsg = s"do not know how this frequency work $freq"
          println(errMsg)
          throw new Exception(errMsg)
      }
    }
  }

  def checkOrderDatetimeFormat(patternDate: String): Boolean = {
    var useBetweenFlag = true

    def checkIndexAndFound(formatIn: String, charFind: String): Int = {
      var maxIndexOnFind: Int = -1
      var tempFormatIn = formatIn
      var currentOffset = 0 // Keep track of characters removed

      var foundIndex = tempFormatIn.indexOf(charFind)
      while (foundIndex != -1) {
        maxIndexOnFind = currentOffset + foundIndex
        tempFormatIn = tempFormatIn.replaceFirst(java.util.regex.Pattern.quote(charFind), "")
        currentOffset += charFind.length // Account for removed characters when updating offset
        foundIndex = tempFormatIn.indexOf(charFind)
      }
      maxIndexOnFind
    }

    var current_index = 0
    var patternDateTemp = patternDate.replace("%%", "") // Handle escaped '%'
    val listOrderToCheck: List[Any] = List(
      List("%Y", "%y"), // Year (full, abbreviated)
      List("%U", "%W"), // Week number (Sunday start, Monday start)
      "%m", // Month
      "%w", // Weekday (0-6)
      "%d", // Day of month
      "%H", // Hour (24-hour)
      "%M", // Minute
      "%S", // Second
      "%f" // Microsecond
    )

    for (itemIn <- listOrderToCheck) {
      if (!useBetweenFlag) { // Early exit if flag is already false
        return false
      }

      itemIn match {
        case items: List[_] => // Handle lists of alternative formats (e.g., %Y or %y)
          var checkingLoopCount = 0
          var foundInSublist = false
          for (item <- items.map(_.asInstanceOf[String])) {
            val indexOut = checkIndexAndFound(patternDateTemp, item)
            if (indexOut != -1) {
              foundInSublist = true // Mark that at least one item from the sublist was found
              if (indexOut >= current_index) {
                current_index = indexOut
              } else {
                if (checkingLoopCount == 0) { // Only set false if it's the first relevant item out of order
                  useBetweenFlag = false
                  // Break logic is handled by the outer loop's check on useBetweenFlag
                }
              }
              // Replace the found item to ensure next search is accurate relative to remaining string
              patternDateTemp = patternDateTemp.replaceFirst(java.util.regex.Pattern.quote(item), "")
              // No need to continue checking other items in this sublist if one is found and processed
              // However, the original Python code continues iterating the inner loop, so we'll mimic that.
            }
            checkingLoopCount += 1
          }
        // If none of the sublist items were found, treat it as "not found" for this level
        // The Python `pass` effectively means no change to useBetweenFlag if not found
        // The critical part for `useBetweenFlag = false` is `index_out < current_index`
        // If no match in sublist, `current_index` doesn't change, which is correct.

        case item: String => // Handle single format specifiers
          val indexOut = checkIndexAndFound(patternDateTemp, item)
          if (indexOut == -1) {
            // Can't Find Any Format - do nothing, as in Python's pass
          } else if (indexOut >= current_index) {
            current_index = indexOut
          } else if (indexOut < current_index) {
            useBetweenFlag = false
          }
          patternDateTemp = patternDateTemp.replaceFirst(java.util.regex.Pattern.quote(item), "")
      }
    }

    // After checking all ordered formats, if any '%' remains, it's an unhandled format
    if (patternDateTemp.contains('%')) {
      useBetweenFlag = false
    }

    println(s"Status Check Flag : $useBetweenFlag")
    useBetweenFlag
  }


  def minusDateByFrequency(dateIn: LocalDateTime, frequency: String, valueFeq: Any = 1): LocalDateTime = {
    var effectiveValueFeq = 0
    if (valueFeq == null || valueFeq.toString.trim.isEmpty) {
      effectiveValueFeq = 0
    } else {
      Try(valueFeq.toString.toInt) match {
        case Success(v) => effectiveValueFeq = v
        case Failure(e) =>
          val errMsg = s"Has Error on config value to calculate date is number $valueFeq. Detail error -> ${e.getMessage}"
          throw new InvalidArgumentException(errMsg)
      }
    }

    var effectiveFrequency = frequency
    if (effectiveFrequency == null || effectiveFrequency.trim.isEmpty) {
      effectiveFrequency = "daily"
    }
    effectiveFrequency = effectiveFrequency.toLowerCase.trim

    if (effectiveFrequency.endsWith("m") && effectiveFrequency.length > 1) { // Check for "Xm" (minutes)
      val valueInStr = effectiveFrequency.dropRight(1).trim
      if (valueInStr.forall(Character.isDigit)) {
        val valueIn = valueInStr.toInt
        dateIn.minus(valueIn.toLong * effectiveValueFeq, ChronoUnit.MINUTES)
      } else {
        val errMsg = s"Has Something Wrong with frequency $frequency"
        println(errMsg)
        throw new InvalidArgumentException(errMsg)
      }
    } else if (effectiveFrequency.endsWith("h") && effectiveFrequency.length > 1) { // Check for "Xh" (hours)
      val valueInStr = effectiveFrequency.dropRight(1).trim
      if (valueInStr.forall(Character.isDigit)) {
        val valueIn = valueInStr.toInt
        dateIn.minus(valueIn.toLong * effectiveValueFeq, ChronoUnit.HOURS)
      } else {
        val errMsg = s"Has Something Wrong with frequency $frequency"
        println(errMsg)
        throw new InvalidArgumentException(errMsg)
      }
    } else if (effectiveFrequency == "hourly") {
      dateIn.minus(1L * effectiveValueFeq, ChronoUnit.HOURS)
    } else if (effectiveFrequency == "daily") {
      dateIn.minus(1L * effectiveValueFeq, ChronoUnit.DAYS)
    } else if (effectiveFrequency == "weekly") {
      dateIn.minus(7L * effectiveValueFeq, ChronoUnit.DAYS)
    } else if (effectiveFrequency == "monthly") {
      dateIn.minus(1L * effectiveValueFeq, ChronoUnit.MONTHS)
    } else if (effectiveFrequency == "quarterly") {
      dateIn.minus(3L * effectiveValueFeq, ChronoUnit.MONTHS)
    } else if (effectiveFrequency == "yearly") {
      dateIn.minus(1L * effectiveValueFeq, ChronoUnit.YEARS)
    } else {
      val errMsg = s"Do not know how this frequency work: $frequency"
      println(errMsg)
      throw new InvalidArgumentException(errMsg)
    }
  }

  def addDateByFrequency(dateIn: LocalDateTime, frequency: String, valueFeq: Any = 1): LocalDateTime = {
    var effectiveValueFeq = 0
    if (valueFeq == null || valueFeq.toString.trim.isEmpty) {
      effectiveValueFeq = 0
    } else {
      Try(valueFeq.toString.toInt) match {
        case Success(v) => effectiveValueFeq = v
        case Failure(e) =>
          val errMsg = s"Has Error on config value to calculate date is number $valueFeq. Detail error -> ${e.getMessage}"
          throw new InvalidArgumentException(errMsg)
      }
    }

    var effectiveFrequency = frequency
    if (effectiveFrequency == null || effectiveFrequency.trim.isEmpty) {
      effectiveFrequency = "daily"
    }
    effectiveFrequency = effectiveFrequency.toLowerCase.trim

    if (effectiveFrequency.endsWith("m") && effectiveFrequency.length > 1) { // Check for "Xm" (minutes)
      val valueInStr = effectiveFrequency.dropRight(1).trim // Remove 'm' and trim
      if (valueInStr.forall(Character.isDigit)) {
        val valueIn = valueInStr.toInt
        dateIn.plus(valueIn.toLong * effectiveValueFeq, ChronoUnit.MINUTES)
      } else {
        val errMsg = s"Has Something Wrong with frequency $frequency"
        println(errMsg)
        throw new InvalidArgumentException(errMsg)
      }
    } else if (effectiveFrequency.endsWith("h") && effectiveFrequency.length > 1) { // Check for "Xh" (hours)
      val valueInStr = effectiveFrequency.dropRight(1).trim
      if (valueInStr.forall(Character.isDigit)) {
        val valueIn = valueInStr.toInt
        dateIn.plus(valueIn.toLong * effectiveValueFeq, ChronoUnit.HOURS)
      } else {
        val errMsg = s"Has Something Wrong with frequency $frequency"
        println(errMsg)
        throw new InvalidArgumentException(errMsg)
      }
    } else if (effectiveFrequency == "hourly") {
      dateIn.plus(1L * effectiveValueFeq, ChronoUnit.HOURS)
    } else if (effectiveFrequency == "daily") {
      dateIn.plus(1L * effectiveValueFeq, ChronoUnit.DAYS)
    } else if (effectiveFrequency == "weekly") {
      dateIn.plus(7L * effectiveValueFeq, ChronoUnit.DAYS)
    } else if (effectiveFrequency == "monthly") {
      dateIn.plus(1L * effectiveValueFeq, ChronoUnit.MONTHS)
    } else if (effectiveFrequency == "quarterly") {
      dateIn.plus(3L * effectiveValueFeq, ChronoUnit.MONTHS)
    } else if (effectiveFrequency == "yearly") {
      dateIn.plus(1L * effectiveValueFeq, ChronoUnit.YEARS)
    } else {
      val errMsg = s"Do not know how this frequency work: $frequency"
      println(errMsg)
      throw new InvalidArgumentException(errMsg)
    }
  }

  def convertPythonDateFormatToJava(pythonPattern: String): String = {
    var pythonPatternTemp = pythonPattern
    if(pythonPatternTemp == null) {
      pythonPatternTemp = "%Y%m%d"
    }
    pythonPatternTemp
      .replace("%Y", "yyyy")
      .replace("%m", "MM")
      .replace("%d", "dd")
      .replace("%H", "HH")
      .replace("%M", "mm")
      .replace("%S", "ss")
      .replace("%f", "SSSSSS") // Milliseconds or nanoseconds depending on precision
      .replace("%%", "%") // Handle escaped %
    // ... add more conversions as needed (e.g., %y, %U, %W, %w)
  }


  def checkExistsDataWithOutCheckMiss(records: Array[Row], targetDate: String,
                                      prerequisiteJobNameStr: String, emptyFlag: Int,
                                      prerequisiteTableCleaned: String): (Boolean, Map[String, List[String]]) = {
    if (records.isEmpty) {
      println("Table is no record.")
      val name = if (prerequisiteJobNameStr.nonEmpty) prerequisiteJobNameStr else prerequisiteTableCleaned
      (false, Map(name -> List(targetDate)))
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
  }

  def checkExistsDataWithCheckMissMaxDate(records: ResultSet, dateQueryStr: String,
                                          prerequisiteJobNm: String, patternIctrlDateCheck: String,
                                          emptyFlag: Int, masterRefDate: LocalDateTime): (Boolean, Map[String, List[String]]) = {
    var successFulRecordsDate: List[LocalDateTime] = List.empty
    var isRecordExists: Boolean = false
    while(records.next()) {
      if(records.getString(4) == "SUCCEED" && records.getLong(5) >= emptyFlag) {
        successFulRecordsDate =successFulRecordsDate :+ parseToLocalDateTime(records.getString(3),patternIctrlDateCheck).get
      }
      isRecordExists = true
    }
    if(!isRecordExists) {
      return (false, Map(prerequisiteJobNm -> List(dateQueryStr)))
    }
    if(successFulRecordsDate.isEmpty) {
      (false, Map(prerequisiteJobNm -> List(dateQueryStr)))
    }
    else {
      val lastDateTime = successFulRecordsDate.maxBy(_.toEpochSecond(java.time.ZoneOffset.UTC))
      if (lastDateTime.isEqual(masterRefDate) || lastDateTime.isAfter(masterRefDate)) {
        println("table is ready.\n")
        (true, Map(prerequisiteJobNm -> List.empty[String]))
      } else {
        println("table is not ready.\n")
        (false, Map(prerequisiteJobNm -> List(dateQueryStr)))
      }
    }
  }

  def checkExistsDataWithCheckMiss(records: ResultSet, listDateTarget: List[String],
                                   prerequisiteJobNameStr: String,
                                   emptyFlag: Int,values: Option[String]): (Boolean, Map[String, List[String]]) = {
    var listLogDate: List[String] = List.empty[String]
    var isRecordExists: Boolean = false
    while(records.next()) {
      if(records.getLong(5) >= emptyFlag && records.getString(4) == "SUCCEED") {
        listLogDate = listLogDate :+ records.getString(3)
      }
      isRecordExists = true
    }
    if(!isRecordExists) {
      return (false, Map(prerequisiteJobNameStr -> listDateTarget))
    }
    if(values != null) {
      val logRequire = 24 / values.map(_.toInt).getOrElse(1)
      val findMiss = listDateTarget.toSet -- listLogDate.toSet
      val numberOfMiss = 24 - logRequire
      if (findMiss.size <= numberOfMiss) {
        logger.info("Table is ready.")
        (true, Map(prerequisiteJobNameStr -> List()))
      } else {
        logger.info("Table is not ready.")
        (false, Map(prerequisiteJobNameStr -> findMiss.toList))
      }
    }
    else {
      val findMiss = listDateTarget.toSet -- listLogDate.toSet
      if (findMiss.isEmpty) {
        logger.info("Table is ready.")
        (true, Map(prerequisiteJobNameStr -> List()))
      } else {
        logger.info("Table is not ready.")
        (false, Map(prerequisiteJobNameStr -> findMiss.toList))
      }
    }
  }

  def updateTaskgroupLogs(tasksgroupNm:String,
                          taskStartTime: LocalDateTime,roundTime: LocalDateTime,
                          status: String,connectionInfo: ConnectionInfo): Unit = {
    val nmTaskgroupLogs = s"$schemaName.tbl_tasksgroup_logs"
    val taskEndTime = LocalDateTime.now()
    val duration = Duration.between(taskStartTime, taskEndTime)
    val hours = duration.toHours
    val minutes = duration.minusHours(hours).toMinutes
    val seconds = duration.minusHours(hours).minusMinutes(minutes).getSeconds
    val durationString = f"$hours%02d:$minutes%02d:$seconds%02d"
    val params = Seq(taskEndTime,durationString,status,tasksgroupNm,roundTime)
    val updateQueryLogs = s"UPDATE $nmTaskgroupLogs set task_end_time = " +
      s"?, duration = ?, status = ? where tasksgroup_nm = ? and round_time = ?"
    ConnectionService.postgresqlInsertUpdateFunc(
      connectionInfo.getIp,
      connectionInfo.getPort,
      connectionInfo.getDbName,
      connectionInfo.getUserNm,
      connectionInfo.getPassword,
      updateQueryLogs,
      params
    )
  }

  def insertTaskgroupLogs(tasksgroupNm: String,
                          taskStartTime: LocalDateTime,
                          roundTime: LocalDateTime,
                          zeppelinName: String,
                          connectionInfo: ConnectionInfo,
                          workflowName: String
                         ): Unit = {

    val nmTaskgroupLogs = s"$schemaName.tbl_tasksgroup_logs"
    val status = "RUNNING"
    val baseZeppelin = zeppelinName
    val params = Seq(tasksgroupNm,workflowName,roundTime,taskStartTime,status,baseZeppelin)
    val queryInsertLog = s"""
    INSERT INTO $nmTaskgroupLogs (tasksgroup_nm,workflow_nm, round_time, task_start_time, task_end_time, duration, status, zeppelin)
    VALUES (?, ?, ?, ?, null, null, ?, ?)
  """

    ConnectionService.postgresqlInsertUpdateFunc(
      connectionInfo.getIp,
      connectionInfo.getPort,
      connectionInfo.getDbName,
      connectionInfo.getUserNm,
      connectionInfo.getPassword,
      queryInsertLog,
      params
    )
  }

  def insertAuditLogDetail(stepRun:String,stepSeq:String,jobName: String,
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
    val sql = s"""INSERT INTO ${this.schemaName}.$auditLogDetail (job_nm,tasksgroup_nm,round_time,dag_run_id,schema_nm,table_nm,load_type,job_start_time,job_end_time,duration,ictrl_dt,step_run,step_seq,status,err_msg)
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

  def checkExistsDataWithOutCheckMiss(records: ResultSet, targetDate: String,
                                      prerequisiteJobNameStr: String,emptyFlag: Int,
                                      prerequisiteTableCleaned: String): (Boolean, Map[String, List[String]]) = {
    while(records.next()) {
      val status = records.getString(4)
      logger.info("status = {}",status)
      val rowCnt = records.getLong(5)
      if (status == "SUCCEED" && rowCnt >= emptyFlag) {
        println("table is ready.")
        return (true, Map(prerequisiteJobNameStr -> List()))
      }
    }
    val name = if (prerequisiteJobNameStr.nonEmpty) prerequisiteJobNameStr else prerequisiteTableCleaned
    (false, Map(name -> List(targetDate)))
  }



  def checkBusinessColumn(
                           frequencyCheck: String,
                           businessColumn: String,
                           prerequisiteSchema: String,
                           prerequisiteTable: String,
                           targetDate: String,
                           sparkSession: SparkSession
                         ): Array[Row] = {

    val queryTableSourceCheck = Map(
      "tbl_source_1_day" ->
        s"""SELECT `$businessColumn` FROM $prerequisiteSchema.$prerequisiteTable WHERE `$businessColumn` = '$targetDate' GROUP BY `$businessColumn`""",
      "tbl_source_multi_day" ->
        s"""SELECT `$businessColumn` FROM $prerequisiteSchema.$prerequisiteTable WHERE `$businessColumn` $targetDate GROUP BY `$businessColumn`""",
      "tbl_source_max_day" ->
        s"""SELECT `$businessColumn` FROM $prerequisiteSchema.$prerequisiteTable GROUP BY `$businessColumn`""",
      "tbl_source_max_day_between" ->
        s"""SELECT `$businessColumn` FROM $prerequisiteSchema.$prerequisiteTable WHERE `$businessColumn` >= '$targetDate' GROUP BY `$businessColumn`"""
    )

    // Determine the query to use based on frequencyCheck
    val query = frequencyCheck match {
      case fc if Set("daily", "weekly", "cur_month", "prev_month", "eom", "day-n", "hourly").contains(fc) =>
        queryTableSourceCheck("tbl_source_1_day")
      case fc if Set(
        "quarter", "month_to_date", "hour_to_date", "date_range", "hour_in_range",
        "daily_period", "date_in_range", "year_to_date", "24hours", "every_n_hour"
      ).contains(fc) =>
        queryTableSourceCheck("tbl_source_multi_day")
      case "max_date" =>
        queryTableSourceCheck("tbl_source_max_day")
      case "max_date_between" =>
        queryTableSourceCheck("tbl_source_max_day_between")
      case _ =>
        throw new IllegalArgumentException(s"Unsupported frequency check type: $frequencyCheck")
    }

    println(s"Query On Data Source: $query")

    val records = sparkSession.sql(query).collect()
    records
  }

}
