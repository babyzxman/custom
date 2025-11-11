package com.gable.templar.zeus.custom

import com.fasterxml.jackson.databind.JsonNode
import com.gable.templar.constant.JobConstant
import com.gable.templar.constant.JobConstant.{CATCHUP_TYPE, JOB_TYPE, LOAD_TYPE, SCHEMA_LIST}
import com.gable.templar.custom.view.{DependencyCheckModel, ExecuteResponse, RunNotebookParallelResult, RunParallelResult}
import com.gable.templar.exception.{DropSuccessJobError, RunNotebookParallelException}
import com.gable.templar.heaven.exception.InvalidArgumentException
import com.gable.templar.zeus.SparkServer
import com.gable.templar.zeus.controller.model.LoginUser
import com.gable.templar.zeus.controller.tablemanage.view.PartitionCondition
import com.gable.templar.zeus.service.spark.TableManageService.PATH_SEPERATOR
import com.gable.templar.zeus.service.spark.{PartitionKey, SparkHiveMetaStoreService, TableManageService}
import com.gable.templar.zeus.service.vector.ConnectionInfo
import com.gable.templar.zeus.util.{Comparator, ComparatorUtil}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.DeltaLog
import io.delta.tables.DeltaTable
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory
import org.springframework.core.task.TaskExecutor
import org.springframework.web.client.HttpServerErrorException.InternalServerError

import java.nio.file.Files
import java.sql.{ResultSet, Timestamp}
import java.text.SimpleDateFormat
import java.time.{Duration, LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, Temporal, TemporalField}
import java.util
import java.util.concurrent.CompletableFuture
import java.util.function.Supplier
import javax.servlet.http.HttpServletRequest
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}
import scala.util.{Failure, Success, Try}

class TransformFw(override val schemaName: String,
                  override val heraUrl: String,
                  override val loginUser: LoginUser,
                  override val sparkSession: SparkSession,
                  override val taskExecutor: TaskExecutor,
                  val tableService: TableManageService,
                  val sparkHiveMetaStoreService: SparkHiveMetaStoreService) extends CustomFw {

  private val logger = LoggerFactory.getLogger(classOf[TransformFw])

  def getVariableSchemaMapNameFromConstant: mutable.Map[String,String] = {
    val returnMap = mutable.Map.empty[String,String]
    for(schemaList <- SCHEMA_LIST.values()) {
      if(schemaName.endsWith("_true_dev"))
        returnMap.put(schemaList.getSchemaVariable,schemaList.getSchemaNameTrueDev)
      else if(schemaName.endsWith("_uat"))
        returnMap.put(schemaList.getSchemaVariable,schemaList.getSchemaNameUat)
      else
        returnMap.put(schemaList.getSchemaVariable,schemaList.getSchemaName)
    }
    returnMap
  }

  def postProcessOutbound(status: String,
                  runId: String, logUrl: String,
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
    if (status.equals("SUCCEED") && (lastSuccessIctrlDt == null || lastSuccessIctrlDt.toInt < ictrlDt.toInt)){
      val sql = s"update ${this.schemaName}.$tblConfName set last_success_ictrl_dt = '$ictrlDt' " +
        s"where job_nm = '${jobName}'"
      ConnectionService.postgresqlInsertUpdateFunc(
        connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
        connectionInfo.getUserNm, connectionInfo.getPassword, sql, Seq.empty)
    }
  }

  def postProcess(status: String,
                  dependencyCheckModel: DependencyCheckModel,
                  errorMsg: String,
                  runId: String, sparkSession: SparkSession,
                  runParallelResult: RunParallelResult,
                  jobStartTime: LocalDateTime, jobEndTime: LocalDateTime,
                  refDate: LocalDateTime, connectionInfo: ConnectionInfo,
                  ictrlDt: String, jobName: String, tblLogName: String,
                  tblConfName: String,lastSuccessIctrlDt: String,
                  rowCount: Long): Unit = {
    val duration = Duration.between(jobStartTime, jobEndTime)
    val hours = duration.toHours
    val minutes = duration.minusHours(hours).toMinutes
    val seconds = duration.minusHours(hours).minusMinutes(minutes).getSeconds
    val durationString = f"$hours%02d:$minutes%02d:$seconds%02d"
    val queryErrorMsgSql = f"select * from ${this.schemaName}.$tblLogName where job_nm = $jobName " +
      f"and dag_run_id = $runId and ictrl_dt = $ictrlDt and round_time = $refDate"
    val result = ConnectionService.postgresqlQueryDirectly(connectionInfo.getIp,
      connectionInfo.getPort, connectionInfo.getDbName,
      connectionInfo.getUserNm, connectionInfo.getPassword, queryErrorMsgSql)
    var hasErrorMsg: Boolean = false
    breakable {
      while (result.rs.next()) {
        if (result.rs.getString("err_msg") != null) {
          hasErrorMsg = true
          break
        }
      }
    }
    if(hasErrorMsg) {
      val params = Seq(status, rowCount,
        objectMapper.writeValueAsString(runParallelResult),
        durationString,jobEndTime,jobEndTime, jobName, runId, ictrlDt, refDate)
      val sql = f"update ${this.schemaName}.$tblLogName set status = ?, " +
        f"row_cnt = ?, log_url = ?, duration = ?,job_end_time = ?,custom_end_time = ? " +
        f"where job_nm = ? and dag_run_id = ? and ictrl_dt = ? " +
        f"and round_time = ?"
      ConnectionService.postgresqlInsertUpdateFunc(connectionInfo.getIp,
        connectionInfo.getPort, connectionInfo.getDbName,
        connectionInfo.getUserNm, connectionInfo.getPassword, sql, params)
    }
    else {
      val params = Seq(status, errorMsg, rowCount,
        objectMapper.writeValueAsString(runParallelResult),
        durationString,jobEndTime,jobEndTime, jobName, runId, ictrlDt, refDate)
      val sql = f"update ${this.schemaName}.$tblLogName set status = ?, " +
        f"err_msg = ?, row_cnt = ?, log_url = ?, duration = ?,job_end_time = ?,custom_end_time = ? " +
        f"where job_nm = ? and dag_run_id = ? and ictrl_dt = ? " +
        f"and round_time = ?"
      ConnectionService.postgresqlInsertUpdateFunc(connectionInfo.getIp,
        connectionInfo.getPort, connectionInfo.getDbName,
        connectionInfo.getUserNm, connectionInfo.getPassword, sql, params)
    }
    if (status.equals("SUCCEED") && (lastSuccessIctrlDt == null || lastSuccessIctrlDt.toInt < ictrlDt.toInt)) {
      val sql = s"update ${this.schemaName}.$tblConfName set last_success_ictrl_dt = '$ictrlDt' " +
        s"where job_nm = '${jobName}'"
      ConnectionService.postgresqlInsertUpdateFunc(
        connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
        connectionInfo.getUserNm, connectionInfo.getPassword, sql, Seq.empty)
    }
  }

  def doRunNotebookParallelInMainOutbound(jobName: String,taskGroupName:String, controlJobDf: Row,runId:String,
                                          roundTime: LocalDateTime,currentLocalDateRun: LocalDateTime,
                                          refDateIctrlDt: String,
                                          connectionInfo: ConnectionInfo,dependencyCheckModel: DependencyCheckModel,
                                          httpServletRequest: HttpServletRequest,username: String,overlap: Any,
                                          frequency: String,ictrlDtTgtfmt: String) : RunNotebookParallelResult = {
    val param: java.util.HashMap[String, JsonNode] = new util.HashMap[String, JsonNode]()
    param.put("job_nm", objectMapper.valueToTree(jobName))
    param.put("tasksgroup_nm", objectMapper.valueToTree(taskGroupName))
    param.put("ref_date", objectMapper.valueToTree(currentLocalDateRun.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"))))
    param.put("ictrl_dt", objectMapper.valueToTree(refDateIctrlDt))
    param.put("start_ictrl_dt", objectMapper.valueToTree(refDateIctrlDt))
    param.put("end_ictrl_dt", objectMapper.valueToTree(refDateIctrlDt))
    param.put("start_ictrl_dt_w_overlap", objectMapper.valueToTree(
      calOverLap(currentLocalDateRun,overlap,frequency).format(DateTimeFormatter.ofPattern(ictrlDtTgtfmt))))
    param.put("end_ictrl_dt_w_overlap", objectMapper.valueToTree(refDateIctrlDt))
    param.put("round_time",objectMapper.valueToTree(roundTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"))))
    param.put("frequency_job", objectMapper.valueToTree(frequency))
    if(dependencyCheckModel.getFixedDate != null)
      param.put("manual_ref_date", objectMapper.valueToTree("False"))
    else
      param.put("manual_ref_date",objectMapper.valueToTree("True"))
    param.put("dag_run_id", objectMapper.valueToTree(runId))
    val jobStartTime: LocalDateTime = LocalDateTime.now()
    updateJobStartTimeOfAuditLogByJobNameAndRoundTimeAndDagRun(
      jobName,jobStartTime,roundTime,runId,
      "tbl_ingest_audit_logs",connectionInfo,refDateIctrlDt)
    doRunNotebookParallel(param,
      "2M73P3JKQ", dependencyCheckModel,
      username, runId,httpServletRequest)
  }

  def doRunNotebookParallelInMain(jobName: String,taskGroupName:String, controlJobDf: Row,runId:String,
                                  roundTime: LocalDateTime,masterRefDate: LocalDateTime,currentLocalDateRun: LocalDateTime,
                                  refDateIctrlDt: String,schemaMap: mutable.Map[String,String],
                                  connectionInfo: ConnectionInfo, JOB_TYPE: JOB_TYPE,dependencyCheckModel: DependencyCheckModel,
                                  httpServletRequest: HttpServletRequest,username: String,overlap: Any,
                                  frequency: String) : RunNotebookParallelResult = {
    val param: java.util.HashMap[String, JsonNode] = new util.HashMap[String, JsonNode]()
    val specArg: util.ArrayList[String] = new util.ArrayList[String]
    specArg.add(refDateIctrlDt)
    specArg.add(jobName)
    specArg.add(taskGroupName)
    specArg.add(controlJobDf.getAs[String]("schema_nm"))
    specArg.add(controlJobDf.getAs[String]("table_nm"))
    specArg.add(runId)
    specArg.add(controlJobDf.getAs[String]("load_type"))
    specArg.add(roundTime.format(DateTimeFormatter.ofPattern(
      "yyyy-MM-dd HH:mm:ss.SSSSSS")))
    specArg.add(masterRefDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")))
    specArg.add(calOverLap(currentLocalDateRun,overlap,frequency).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")))
    specArg.add(addDateByFrequency(masterRefDate,frequency).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")))
    if (controlJobDf.getAs[String]("specific_argument") != null) {
      val newArgs = controlJobDf.getAs[String]("specific_argument").replace("'", "\"")
      val specificArg = scalaObjectMapper.readValue(
        newArgs, classOf[List[Any]])
      if (specificArg.nonEmpty) {
        specArg.add(specificArg.get(0).toString)
      }
      else {
        specArg.add("1")
      }
    }
    else {
      specArg.add("1")
    }
    param.put("fix_date", objectMapper.valueToTree(refDateIctrlDt))
    param.put("back_date", objectMapper.valueToTree(0))
    param.put("job_name", objectMapper.valueToTree(jobName))
    param.put("spec_arg", objectMapper.valueToTree(specArg))
    var globalParamMap = ConnectionService.getGlobalParams(
      connectionInfo.getIp,connectionInfo.getPort,connectionInfo.getDbName,
      connectionInfo.getUserNm,connectionInfo.getPassword,
      s"select * from $schemaName.tbl_global_params where system = 'schema'")
    for(schemaEntry <- globalParamMap.entrySet()) {
      param.put(schemaEntry.getKey,objectMapper.valueToTree(schemaEntry.getValue))
    }
//    if(dependencyCheckModel.getWorkspaceName != null) {
//      globalParamMap = ConnectionService.getGlobalParams(
//        connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
//        connectionInfo.getUserNm, connectionInfo.getPassword,
//        s"select * from $schemaName.tbl_global_params where system = 'schema' and work_space = '${dependencyCheckModel.getWorkspaceName}'")
//      for(schemaEntry <- globalParamMap.entrySet()) {
//        param.put(schemaEntry.getKey,objectMapper.valueToTree(schemaEntry.getValue))
//      }
//    }
//    globalParamMap = ConnectionService.getGlobalParams(
//      connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
//      connectionInfo.getUserNm, connectionInfo.getPassword,
//      s"select * from $schemaName.tbl_global_params where system = 'schema' and notebook_name = '${controlJobDf.getAs[String]("script_path").trim}'")
//    for(schemaEntry <- globalParamMap.entrySet()) {
//      param.put(schemaEntry.getKey,objectMapper.valueToTree(schemaEntry.getValue))
//    }
    val jobStartTime: LocalDateTime = LocalDateTime.now()
    updateJobStartTimeOfAuditLogByJobNameAndRoundTimeAndDagRun(
      jobName,jobStartTime,roundTime,runId,
      "tbl_ingest_audit_logs",connectionInfo,refDateIctrlDt)
    doRunNotebookParallelRunWithName(param,
        controlJobDf.getAs[String]("script_path").trim, dependencyCheckModel,
        username, runId,httpServletRequest)
  }

  def getTablePartitionDropList(partitionConds: List[PartitionCondition],
                                tableName: String,
                                partitionKeys: List[PartitionKey]): List[String] = {

    val partitionKeysName = partitionKeys.map(_.name)
    var ptConditionsGroup = Map[String, List[PartitionCondition]]()
    if (null != partitionConds && partitionConds.nonEmpty) {
      ptConditionsGroup = partitionConds.groupBy(_.getName)
      ptConditionsGroup.keys.foreach(ptCondName => {
        if (!partitionKeysName.contains(ptCondName)) {
          throw new IllegalArgumentException(s"partition condition name ${ptCondName} is not defined as table partitions")
        }
      })
    }
    var dropPartitionList = List[String]()
    if (null != partitionKeys && partitionKeys.nonEmpty) {
      dropPartitionList = listPartitionsByFilter(tableName, ptConditionsGroup, partitionKeys)
    }
    dropPartitionList
  }

  def listPartitionsByFilter(tableName: String, ptConditionsGroup: Map[String, List[PartitionCondition]], partitionsKeys: List[PartitionKey]) = {
    var existsPartitionList = sparkHiveMetaStoreService.getPartitionList(tableName)
    /** find the deepest partition which is not considered by any conditions and split it out
     * eg. dt=[]/dt_hour=[]/dt_min=[]/dt_sec=[]
     * if filter condition deepest level is 'dt_min', should trim out 'dt_sec'
     */
    val conditionsKey = ptConditionsGroup.keys.toSet
    var ignorePartition: String = null
    if (ptConditionsGroup.nonEmpty) {
//      breakable {
//        for (pname <- partitionsKeys.map(_.name).reverse) {
//          if (!conditionsKey.contains(pname)) {
//            ignorePartition = pname
//          } else {
//            break
//          }
//        }
//      }
    }
//    if (ignorePartition != null) {
//      existsPartitionList = existsPartitionList.map(p => p.slice(0, p.indexOf(ignorePartition) - 1)).distinct
//    }

    existsPartitionList = existsPartitionList.filter(existsPart => {
      isPartitionInCondition(existsPart, ptConditionsGroup, partitionsKeys)
    })
    existsPartitionList
  }

  def isPartitionInCondition(existsPart: String, ptConditionsGroup: Map[String, List[PartitionCondition]], partitionsKeys: List[PartitionKey]): Boolean = {
    val existsPartArray = leftTrim(existsPart,"/").split("/")
    for (i <- 0 until existsPartArray.length) {
      //extract value from each exists partition eg. dt=201902801
      logger.debug(s"partition index ${i}: " + existsPartArray(i))
      val subPartValue = existsPartArray(i).split("=")(1)
      if (!ptConditionsGroup.contains(partitionsKeys(i).name)) {
        // no filter for this partition level -> continue
      } else {
        // get all filter condition for this partition level
        val partitionCondList = ptConditionsGroup.get(partitionsKeys(i).name).get
        partitionCondList.foreach(partitionCond => {
          val isSubPartInCondition = isSubPartitionInCondition(subPartValue, partitionCond, partitionsKeys(i))
          logger.debug(s"partition index ${i}: compare ${subPartValue} - ${partitionCond.getValue}: ${isSubPartInCondition}")
          if (!isSubPartInCondition) {
            return false
          }
        })
      }
    }
    return true
  }

  private def isSubPartitionInCondition(subPartValue: String, partitionCond: PartitionCondition, partitionsKeys: PartitionKey): Boolean = {
    try {
      if (partitionsKeys.ptype == "int") {
        return ComparatorUtil.compareNumeric(subPartValue.toInt, partitionCond.getValue.toInt, partitionCond.getComparator)
      } else if (partitionsKeys.ptype == "bigint") {
        return ComparatorUtil.compareNumeric(subPartValue.toLong, partitionCond.getValue.toLong, partitionCond.getComparator)
      } else if (partitionsKeys.ptype == "double") {
        return ComparatorUtil.compareNumeric(subPartValue.toDouble, partitionCond.getValue.toDouble, partitionCond.getComparator)
      } else if (partitionsKeys.ptype == "float") {
        return ComparatorUtil.compareNumeric(subPartValue.toFloat, partitionCond.getValue.toFloat, partitionCond.getComparator)
      } else {
        if (partitionCond.getComparator != null && partitionCond.getComparator.trim != "" && Comparator.EQUAL.name != partitionCond.getComparator) {
          throw new Exception(s"partition comparator ${partitionCond.getComparator} is not supported for [${partitionsKeys.name}]: ${partitionsKeys.ptype} data type")
        }
        return (subPartValue == partitionCond.getValue)
        //        return subPartValue == URLEncoder.encode(partitionCond.getValue, "UTF-8")
      }
    } catch {
      case ne: NumberFormatException => {
        return false
      }
      case e: Exception => {
        throw e
      }
    }
    return true
  }

  private def leftTrim(str: String, char: String): String = {
    if(str.startsWith(char)) {
      println(str.indexOf(char))
      str.substring(str.indexOf(char)+1)
    } else {
      str
    }
  }


  def deletePartition(path: String,
                      partitionConds:List[PartitionCondition],
                      tableName: String,sparkSession: SparkSession,
                      partitionKeys: List[PartitionKey],
                      dropPartitionList: List[String]): Unit = {
    val partitionKeysName = partitionKeys.map(_.name)
    var ptConditionsGroup = Map[String, List[PartitionCondition]]()
    if (null != partitionConds && partitionConds.nonEmpty) {
      ptConditionsGroup = partitionConds.groupBy(_.getName)
      ptConditionsGroup.keys.foreach(ptCondName => {
        if (!partitionKeysName.contains(ptCondName)) {
          throw new IllegalArgumentException(s"partition condition name ${ptCondName} is not defined as table partitions")
        }
      })
    }
    if (null != partitionKeys && partitionKeys.nonEmpty) {
      dropPartitionList.foreach(partToDrop => {
        //				zeusSession.sql(s"ALTER table ${tableName} DROP IF EXISTS partition (${partToDrop.replaceAll("/", ",")})")
        sparkSession.sql(s"ALTER table ${tableName} DROP IF EXISTS partition (${tableService.convertToPartitionSql(partToDrop)})")
      })
    }
    val deletedSrcPaths = dropPartitionList.map(partToDrop => path + PATH_SEPERATOR + partToDrop)
    deletedSrcPaths.foreach(p => {
      SparkServer.getStoreUtil().delete(p,true)
    })
  }

  def getTableIdentifierFromTableName(tableName: String): TableIdentifier = {
    if (tableName.contains(".")) {
      val tableNameSplit = tableName.split("\\.")
      new TableIdentifier(tableNameSplit(1), Some(tableNameSplit(0)))
    }
    else {
      new TableIdentifier(tableName)
    }
  }

  def generateDeltaLogWherePartitionCondition(tableName: String,sparkSession: SparkSession,
                                              partitionConditions: List[PartitionCondition]): List[String] = {
    val snap = DeltaLog.forTable(sparkSession,getTableIdentifierFromTableName(tableName)).snapshot
    var deletePartitionList: List[String] = List.empty
    var it:Iterator[AddFile] = null
    if(partitionConditions.isEmpty) {
      it = snap.allFiles
        .toLocalIterator()    // Iterator[Row], not AddFile
    }
    else {
      val whereCondition: StringBuilder = StringBuilder.newBuilder
      var count = 0
      for(partitionCondition <- partitionConditions) {
        whereCondition.append(f"partitionValues['${partitionCondition.getName}'] ${
          PartitionParsers.convertComparatorToSymbol(partitionCondition.getComparator)} " +
          f"'${partitionCondition.getValue}'")
        if(count < partitionConditions.size - 1) {
          whereCondition.append(" AND ")
        }
        count += 1
      }
      it = snap.allFiles.where(whereCondition.toString()).toLocalIterator()
    }
    while(it.hasNext) {
      val partitionValue = it.next().partitionValues
      val fullPartitionPath: StringBuilder = new StringBuilder()
      var count = 0
      partitionValue.foreach(m => {
        fullPartitionPath.append(f"${m._1}=${m._2}")
        if(count < partitionValue.size -1) {
          fullPartitionPath.append(PATH_SEPERATOR)
        }
        count += 1
      })
      deletePartitionList = deletePartitionList :+ fullPartitionPath.toString()
    }
    deletePartitionList
    // Extract partition maps from rows
  }


  override def doRunFramework(dependencyCheckModel: DependencyCheckModel,
                              JOB_TYPE: JOB_TYPE, jobName: String,
                              controlJobDf: Row,tblConfName: String,
                              httpServletRequest: HttpServletRequest,
                              username: String,roundTime: LocalDateTime): CompletableFuture[ExecuteResponse]= {
    val completableFuture: CompletableFuture[ExecuteResponse] = CompletableFuture.supplyAsync(new Supplier[ExecuteResponse] {
      override def get(): ExecuteResponse =  {
        if(dependencyCheckModel.get_bldEndDate() == null) {
          dependencyCheckModel.set_bldEndDate(new Timestamp(System.currentTimeMillis()))
        }
        if(dependencyCheckModel.getModuleNotebookName == null)
          dependencyCheckModel.setModuleNotebookName("zeppelin-se-uat-g")
        var masterRefDate: LocalDateTime = null
        var processJobType: String = "ongoing"
        var frequency = controlJobDf.getAs[String]("frequency")
        val backdate = controlJobDf.getAs[Any]("back_day")
        var currentLocalDateRun: LocalDateTime = null
        var catchUpType = controlJobDf.getAs[String]("catchup_type")
        var ictrlDtTgtFmt = controlJobDf.getAs[String]("ictrl_dt_tgtfmt")
//        val isSkipCatchUp = (controlJobDf.getAs[String]("ignore_catchup") != null &&
//          controlJobDf.getAs[String]("ignore_catchup") == "Y")
        var schemaNameFromTbl: String = null
        var tableName: String = null
        var loadType: String = null
        if(JOB_TYPE.equals(JobConstant.JOB_TYPE.TRANSFORM)) {
          schemaNameFromTbl = controlJobDf.getAs[String]("schema_nm")
          tableName = controlJobDf.getAs[String]("table_nm")
          loadType = controlJobDf.getAs[String]("load_type")
        }
        else {
          schemaNameFromTbl = controlJobDf.getAs[String]("ob_src_schema")
          tableName = controlJobDf.getAs[String]("ob_src_table")
          loadType = controlJobDf.getAs[String]("ob_write_mode")
        }
        var taskGroupName = controlJobDf.getAs[String]("tasksgroup_nm")
        val overlapTime = controlJobDf.getAs[Any]("overlap")
        if(taskGroupName == null ) {
          taskGroupName = ""
        }
        if(ictrlDtTgtFmt == null) {
          ictrlDtTgtFmt = "%Y%m%d"
        }
        if(catchUpType == null) {
          catchUpType = CATCHUP_TYPE.SEQUENCE.getValue
        }
        val dateFormatIctrlDtForTb = convertPythonDateFormatToJava(ictrlDtTgtFmt)
        val timeRetry = controlJobDf.getAs[Int]("time_retry")
        val totalRetry = controlJobDf.getAs[Int]("total_retry")
        var currentDateRun = controlJobDf.getAs[String]("last_success_ictrl_dt")
        val scheduleCutOff = if(controlJobDf.getAs[String]("retry_timeout") != null) {
          LocalTime.parse(controlJobDf.getAs[String]("retry_timeout"),DateTimeFormatter.ofPattern("HH:mm"))
        }
        else {
          null
        }
        val lastSuccessIctrlDt = currentDateRun
        val queryMasterSql = s"SELECT system,key,values FROM $schemaName.tbl_master_config where system = 'fw_postgre'"
        val connectionInfo = ConnectionService.getMasterConfigLog(queryMasterSql,salt,ultKey)
        val isFrequencyAccordToFmt = checkFrequencyAndTgtFmt(frequency,dateFormatIctrlDtForTb)
        if(dependencyCheckModel.getFixedDate == null || dependencyCheckModel.getFixedDate.isEmpty) {
          masterRefDate = minusDateByFrequency(
            dependencyCheckModel.get_bldEndDate().toLocalDateTime,frequency,backdate)
        }
        else {
          try{
            masterRefDate = parseToLocalDateTime(dependencyCheckModel.getFixedDate,"yyyyMMdd").get
          }
          catch {
            case ex: Exception => {
              masterRefDate = parseToLocalDateTime(dependencyCheckModel.getFixedDate,dateFormatIctrlDtForTb).get
            }
          }
          processJobType = "manual"
        }
//        val isSkipCatchUp = (controlJobDf.getAs[String]("ignore_catchup") != null &&
//          controlJobDf.getAs[String]("ignore_catchup") == "Y")
        if(currentDateRun == null || LOAD_TYPE.FULL_LOAD.getValue.equals(loadType)) {
          currentLocalDateRun = masterRefDate
          currentDateRun = masterRefDate.format(DateTimeFormatter.ofPattern(dateFormatIctrlDtForTb))
        }
        else {
          currentLocalDateRun = parseToLocalDateTime(currentDateRun,dateFormatIctrlDtForTb).get
          currentLocalDateRun = addDateByFrequency(currentLocalDateRun,frequency)
          if(currentLocalDateRun.isAfter(masterRefDate)) {
            currentLocalDateRun = masterRefDate
          }
        }
        val sb = new StringBuilder()
        var isContinueRunning: Boolean = true
        var ictrlDtRun: LocalDateTime = null
        val executeResponse: ExecuteResponse = new ExecuteResponse
        val pattern = "^(.*?)__".r
        val modifiedDagRun = s"${pattern.findFirstMatchIn(
          dependencyCheckModel.get_runId()).map(_.group(1)).getOrElse("")}__${dependencyCheckModel.
          get_bldEndDate().toLocalDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss"))}"
        if(isFrequencyAccordToFmt) {
          frequency = checkFrequencyAndChangeFrequencyToCorrectFrequency(frequency, dateFormatIctrlDtForTb)
        }
        executeResponse.setJobName(jobName)
        breakable {
          while (isContinueRunning) {
            var runId = ""
            var endIctrlDt: String = ""
            if (dependencyCheckModel.get_workflowId() != null) {
              runId = dependencyCheckModel.get_workflowId() + "|" + modifiedDagRun +
                "|" + dependencyCheckModel.get_taskId()
            }
            else {
              runId = "manual_run_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HH_mm_ss"))
            }
            ictrlDtRun = masterRefDate
            if (catchUpType.equalsIgnoreCase(CATCHUP_TYPE.SEQUENCE.getValue)) {
              if (masterRefDate.isAfter(currentLocalDateRun) || masterRefDate.isEqual(currentLocalDateRun)) {
                ictrlDtRun = currentLocalDateRun
                endIctrlDt = currentLocalDateRun.format(DateTimeFormatter.ofPattern(dateFormatIctrlDtForTb))
              }
              else if (currentLocalDateRun.isAfter(masterRefDate)) {
                isContinueRunning = false
                break()
              }
            }
            else {
              endIctrlDt = masterRefDate.format(DateTimeFormatter.ofPattern(dateFormatIctrlDtForTb))
              isContinueRunning = false
            }
            val refDateIctrlDt = ictrlDtRun.format(DateTimeFormatter.ofPattern(dateFormatIctrlDtForTb))
            val runTime: LocalDateTime = LocalDateTime.now()
            val startIctrlDt = calOverLap(currentLocalDateRun,overlapTime,frequency).format(
              DateTimeFormatter.ofPattern(dateFormatIctrlDtForTb))
            insertRoundAuditLog(dependencyCheckModel,
              schemaNameFromTbl, tableName,
              runId, refDateIctrlDt, connectionInfo, runTime,
              startIctrlDt, endIctrlDt, roundTime, jobName,loadType,
              taskGroupName)
            try {
              checkRunningIctrlDt(
                dependencyCheckModel.getJobName,catchUpType, dateFormatIctrlDtForTb,
                frequency,currentLocalDateRun,masterRefDate,connectionInfo,runId,
                roundTime,processJobType)
              var startDetailTime = LocalDateTime.now()
              val schemaMap = getVariableSchemaMapNameFromConstant
              breakable {
                for (i <- 0 to totalRetry) {
                  val results: java.util.Map[String, Boolean] = checkDependencyByJobName(
                    controlJobDf, ictrlDtRun, connectionInfo,
                    refDateIctrlDt, currentDateRun, null, jobName,schemaMap)
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
                    if(scheduleCutOff != null) {
                      if(LocalTime.now().isAfter(scheduleCutOff)) {
                        throw new InvalidArgumentException("The time retry is more than schedule cutoff time")
                      }
                    }
                    if (i == totalRetry - 1)
                      throw new InvalidArgumentException("The dependency job check failed because this job = " + String.join(",", notReadyJob) + " is not finished")
                  }
                  else {
                    break()
                  }
                  updateJobStatusAndErrorMessage(jobName,roundTime,runId,
                    "tbl_trans_audit_logs",connectionInfo,"WAITING","wailting depen",refDateIctrlDt)
                  Thread.sleep(timeRetry * 1000)
                  updateJobStatusAndErrorMessage(jobName,roundTime,runId,
                    "tbl_trans_audit_logs",connectionInfo,"running",null,refDateIctrlDt)
                }
              }
              var stepRun = "FW CHECK PREREQUISITE"
              var stepSeq = "FW:1"
              var stepRunNext = "FW RUN SCRIPTS TRANSFORMATION"
              var stepSeqNext = "FW:2"
              insertAuditLogDetail(stepRun,stepSeq,jobName,runId,taskGroupName,schemaNameFromTbl,
                tableName,loadType,roundTime,startDetailTime,"SUCCEED",refDateIctrlDt,
                stepRunNext,stepSeqNext,connectionInfo,"tbl_trans_audit_detail_logs",
                "tbl_trans_audit_detail_next_logs")
              startDetailTime = LocalDateTime.now()
              var status = "SUCCEED"
              var isTmpTableExists = false
              val jobStartTime: LocalDateTime = LocalDateTime.now()
              val runParallelResult: RunParallelResult = new RunParallelResult
              var partitionConditionList: List[PartitionCondition] = List.empty
              if(JOB_TYPE.equals(JobConstant.JOB_TYPE.TRANSFORM)) {
                val backlogTime = controlJobDf.getAs[Int]("backlog_times")
                val partitionCondMaster = controlJobDf.getAs[String]("partition_condition")
                if (partitionCondMaster != null) {
                  partitionConditionList = PartitionParsers.parseAnd(partitionCondMaster)
                }
                if (backlogTime != null && backlogTime != 0) {
                  val successList: util.ArrayList[String] = new util.ArrayList
                  runParallelResult.setSuccessList(successList)
                  for (countRemove <- 0 to backlogTime) {
                    val localDateRun = minusDateByFrequency(currentLocalDateRun, frequency, backlogTime - countRemove)
                    val endDate = minusDateByFrequency(ictrlDtRun,frequency, backlogTime - countRemove)
                    val refIctrlDt = localDateRun.format(DateTimeFormatter.ofPattern(dateFormatIctrlDtForTb))
                    val runNotebookParallelResult = doRunNotebookParallelInMain(
                      jobName, taskGroupName, controlJobDf, runId, roundTime, endDate,
                      localDateRun, refIctrlDt, schemaMap, connectionInfo, JOB_TYPE,
                      dependencyCheckModel, httpServletRequest, username, overlapTime, frequency)
                    if (runNotebookParallelResult.getErrorMsg != null) {
                      status = "FAILED"
                      val failedList: util.ArrayList[String] = new util.ArrayList
                      failedList.add(runNotebookParallelResult.getNotebookUrl)
                      runParallelResult.setFailedList(failedList)
                      postProcess(status, dependencyCheckModel,
                        runNotebookParallelResult.getErrorSpecificMsg,
                        runId, sparkSession,
                        runParallelResult, jobStartTime,
                        LocalDateTime.now(), roundTime, connectionInfo, refDateIctrlDt, jobName,
                        "tbl_trans_audit_logs", tblConfName, lastSuccessIctrlDt, 0L)
                      throw new RunNotebookParallelException(runNotebookParallelResult.getErrorMsg)
                    }
                    else {
                      sb.append(runNotebookParallelResult.getMessage)
                      successList.add(runNotebookParallelResult.getNotebookUrl)
                    }
                  }
                }
                else {
                  val successList: util.ArrayList[String] = new util.ArrayList
                  val runNotebookParallelResult = doRunNotebookParallelInMain(
                    jobName, taskGroupName, controlJobDf, runId, roundTime,
                    ictrlDtRun, currentLocalDateRun,
                    refDateIctrlDt, schemaMap, connectionInfo, JOB_TYPE,
                    dependencyCheckModel, httpServletRequest, username,
                    overlapTime, frequency)
                  if (runNotebookParallelResult.getErrorMsg != null) {
                    status = "FAILED"
                    val failedList: util.ArrayList[String] = new util.ArrayList
                    failedList.add(runNotebookParallelResult.getNotebookUrl)
                    postProcess(status, dependencyCheckModel,
                      runNotebookParallelResult.getErrorSpecificMsg,
                      runId, sparkSession,
                      runParallelResult, jobStartTime,
                      LocalDateTime.now(), roundTime, connectionInfo, refDateIctrlDt, jobName,
                      "tbl_trans_audit_logs", tblConfName, lastSuccessIctrlDt, 0L)
                    throw new RunNotebookParallelException(runNotebookParallelResult.getErrorMsg)
                  }
                  sb.append(runNotebookParallelResult.getMessage)
                  successList.add(runNotebookParallelResult.getNotebookUrl)
                  runParallelResult.setSuccessList(successList)
                }
                var processCount = 0L
                val whereCondition = DataValidator.generateWhereConditionFromPartitionCondition(partitionConditionList)
                if(sparkSession.catalog.tableExists(f"$tmpzSchema.${tableName}_tmp_validation")) {
                  sparkSession.catalog.refreshTable(f"$tmpzSchema.${tableName}_tmp_validation")
                  isTmpTableExists = !sparkSession.table(f"$tmpzSchema.${tableName}_tmp_validation").isEmpty
                }
                var partitionCol: List[String] = List.empty
                if(controlJobDf.getAs[String]("partition_column") != null) {
                  partitionCol = scalaObjectMapper.readValue(
                    controlJobDf.getAs[String]("partition_column").replace("'", "\""), classOf[List[String]])
                }
                var updateCondition: List[String] = List.empty
                if(controlJobDf.getAs[String]("update_condition") != null) {
                  updateCondition = scalaObjectMapper.readValue(
                    controlJobDf.getAs[String]("update_condition").replace("'", "\""), classOf[List[String]])
                }
                stepRun = "FW RUN SCRIPTS TRANSFORMATION"
                stepSeq = "FW:2"
                stepRunNext = "FW VALIDATION PROCESS"
                stepSeqNext = "FW:3"
                insertAuditLogDetail(stepRun,stepSeq,jobName,runId,taskGroupName,schemaNameFromTbl,
                  tableName,loadType,roundTime,startDetailTime,"SUCCEED",refDateIctrlDt,
                  stepRunNext,stepSeqNext,connectionInfo,"tbl_trans_audit_detail_logs",
                  "tbl_trans_audit_detail_next_logs")
                startDetailTime = LocalDateTime.now()
                if(isTmpTableExists) {
                  var partitionKeys: List[PartitionKey] = List.empty
                  val isDeltaTable = DeltaTable.isDeltaTable(sparkSession,f"$tmpzSchema.${tableName}_tmp_validation")
                  val dropPartitionList = if(isDeltaTable) {
                    generateDeltaLogWherePartitionCondition(
                      f"$tmpzSchema.${tableName}_tmp_validation",
                      sparkSession,partitionConditionList)
                  }
                  else {
                    partitionKeys = sparkHiveMetaStoreService.getPartitionKeys(f"$tmpzSchema.${tableName}_tmp_validation")
                    getTablePartitionDropList(partitionConditionList,
                      f"$tmpzSchema.${tableName}_tmp_validation",partitionKeys)
                  }
                  processCount = sparkSession.sql(f"select * from $tmpzSchema.${tableName}_tmp_validation $whereCondition").count()
                  val sql = f"select job_nm,schema_nm,tbl_nm,rule_nm,rule_calc," +
                    f"rule_calc_apply_col,expect_value,operation,sql_calc,sql_calc_apply," +
                    f"validate_mode,margin_pct,validate_seq  from $schemaName.tbl_validation " +
                    f"where job_nm = '$jobName' and active_flag = 'Y' order by validate_seq"
                  val validateDf = ConnectionService.postgresqlQueryDirectly(connectionInfo.getIp, connectionInfo.getPort
                    , connectionInfo.getDbName, connectionInfo.getUserNm, connectionInfo.getPassword, sql)
                  try {
                    DataValidator.processValidationRows(sparkSession, validateDf.rs,
                      "_tmp_validation", loadType,whereCondition)
                  }
                  finally {
                    validateDf.close()
                  }
                  val location =
                    sparkSession.sessionState.catalog.getTableMetadata(
                      TableIdentifier(f"${tableName}_tmp_validation",Some(tmpzSchema))).location
                  DataValidator.insertToTargetMode(controlJobDf.getAs[String]("schema_nm"),
                    controlJobDf.getAs[String]("table_nm"), "tmp_validation",
                    controlJobDf.getAs[String]("load_type"), partitionCol, controlJobDf.
                      getAs[String]("unique_key"),
                    Option(controlJobDf.getAs[String]("update_condition")),
                    jobName, connectionInfo, sparkSession, tmpzSchema,updateCondition,
                    whereCondition,location.toString,dropPartitionList,partitionConditionList)
                  logger.info("drop partition list = {}",dropPartitionList)
                  if(isDeltaTable) {
                    sparkSession.sql(s"delete from ${tmpzSchema}.${tableName}_tmp_validation ${DataValidator.generateWhereConditionFromPartitionCondition(partitionConditionList)}")
                  }
                  deletePartition(location.toString,partitionConditionList,
                    f"$tmpzSchema.${tableName}_tmp_validation",sparkSession,
                    partitionKeys,dropPartitionList)
                }
                postProcess(status, dependencyCheckModel, null,
                  runId, sparkSession, runParallelResult, jobStartTime,
                  LocalDateTime.now(), roundTime, connectionInfo, refDateIctrlDt, jobName,
                  "tbl_trans_audit_logs", tblConfName,lastSuccessIctrlDt,processCount)
                stepRun = "FW VALIDATION PROCESS"
                stepSeq = "FW:3"
                stepRunNext = "FW NOTIFICATION SENT"
                stepSeqNext = "FW:4"
                insertAuditLogDetail(stepRun,stepSeq,jobName,runId,taskGroupName,schemaNameFromTbl,
                  tableName,loadType,roundTime,startDetailTime,"SUCCEED",refDateIctrlDt,
                  stepRunNext,stepSeqNext,connectionInfo,"tbl_trans_audit_detail_logs",
                  "tbl_trans_audit_detail_next_logs")
                //              Notifier.notifierCore(sparkSession,jobName, status, refDateIctrlDt,
                //                runNotebookParallelResult.getErrorSpecificMsg, tableName,
                //                controlJobDf.getAs[String]("noti_token"), "''",
                //                controlJobDf.getAs[String]("noti_enable"), delay_flag = delay_flag,
                //                last_delay_day = last_delay_day, web_hook_url = '',
                //                sender = '', password = '', recipients = '', mode = job_type)
              }
              else {
                val successList: util.ArrayList[String] = new util.ArrayList
                val runNotebookParallelResult = doRunNotebookParallelInMainOutbound(
                  jobName, taskGroupName, controlJobDf, runId, roundTime, currentLocalDateRun,
                  refDateIctrlDt, connectionInfo,
                  dependencyCheckModel, httpServletRequest, username,
                  overlapTime, frequency,dateFormatIctrlDtForTb)
                if (runNotebookParallelResult.getErrorMsg != null) {
                  status = "FAILED"
                  val failedList: util.ArrayList[String] = new util.ArrayList
                  failedList.add(runNotebookParallelResult.getNotebookUrl)
                  postProcessOutbound(status,runId,
                    runNotebookParallelResult.getNotebookUrl,jobStartTime,
                    LocalDateTime.now(),roundTime,connectionInfo,refDateIctrlDt,jobName,
                    "tbl_trans_audit_logs",tblConfName,lastSuccessIctrlDt)
                  throw new RunNotebookParallelException(runNotebookParallelResult.getErrorMsg)
                }
                sb.append(runNotebookParallelResult.getMessage)
                successList.add(runNotebookParallelResult.getNotebookUrl)
                runParallelResult.setSuccessList(successList)
                stepRun = "FW RUN SCRIPTS TRANSFORMATION"
                stepSeq = "FW:2"
                stepRunNext = null
                stepSeqNext = null
                insertAuditLogDetail(stepRun,stepSeq,jobName,runId,taskGroupName,schemaNameFromTbl,
                  tableName,loadType,roundTime,startDetailTime,"SUCCEED",refDateIctrlDt,
                  stepRunNext,stepSeqNext,connectionInfo,"tbl_trans_audit_detail_logs",
                  "tbl_trans_audit_detail_next_logs")
                postProcessOutbound(status,runId,
                  runNotebookParallelResult.getNotebookUrl,jobStartTime,
                  LocalDateTime.now(),roundTime,connectionInfo,refDateIctrlDt,jobName,
                  "tbl_trans_audit_logs",tblConfName,lastSuccessIctrlDt)
              }
              if (catchUpType.equalsIgnoreCase(CATCHUP_TYPE.SEQUENCE.getValue)) {
                currentLocalDateRun = addDateByFrequency(currentLocalDateRun, frequency)
                if (currentLocalDateRun.isAfter(masterRefDate)) {
                  isContinueRunning = false
                }
              }
            }
            catch {
              case exception: Exception => {
                logger.error(exception.getMessage, exception)
                if (!exception.getClass.equals(classOf[RunNotebookParallelException])) {
                  updateStateOfAuditLogByJobNameAndRoundTimeAndDagRun(
                    "FAILED",
                    dependencyCheckModel, runId, LocalDateTime.now(),
                    roundTime, connectionInfo, exception.getMessage, "tbl_trans_audit_logs",
                    jobName,refDateIctrlDt)
                }
                throw new Exception(exception.getMessage)
              }
              case dropSuccessJobError: DropSuccessJobError => {
                logger.error(dropSuccessJobError.getMessage, dropSuccessJobError)
                updateStateOfAuditLogByJobNameAndRoundTimeAndDagRun(
                  "FAILED",
                  dependencyCheckModel, runId, LocalDateTime.now(),
                  roundTime, connectionInfo, dropSuccessJobError.getMessage, "tbl_trans_audit_logs",
                  jobName,refDateIctrlDt)
                throw new DropSuccessJobError(dropSuccessJobError.getMessage)
              }
            }
          }
        }
        executeResponse.setMessage(sb.toString())
        executeResponse
      }
    },taskExecutor)
    completableFuture
  }

  def insertRoundAuditLog(dependencyCheckModel: DependencyCheckModel,
                          schemaName: String, tableName: String, runId: String,
                          ictrlDt:String,connectionInfo: ConnectionInfo,jobStartTime: LocalDateTime,
                          startIctrlDt:String,endIctrlDt: String,
                          roundTime: LocalDateTime,jobName: String,
                          loadType:String,taskGroupNm:String): Unit = {
    val seqValue = Seq(jobName,roundTime,runId,
      schemaName,tableName,jobStartTime,ictrlDt,startIctrlDt,ictrlDt,
      dependencyCheckModel.getModuleNotebookName,loadType,taskGroupNm)
    val sql = f"insert into ${this.schemaName}.tbl_trans_audit_logs (job_nm,round_time," +
      f"dag_run_id,schema_nm,table_nm,job_start_time,ictrl_dt,start_ictrl_dt," +
      f"end_ictrl_dt,status,zeppelin,load_type,tasksgroup_nm) values (" +
      f"?,?,?,?,?," +
      f"?,?,?,?,'RUNNING'," +
      f"?,?,?)"
    ConnectionService.postgresqlInsertUpdateFunc(connectionInfo.getIp,connectionInfo.getPort,connectionInfo.getDbName,
      connectionInfo.getUserNm,connectionInfo.getPassword,sql,seqValue)
  }

  def overwriteFromTempValidationToParentTable(schemaName: String, tableName: String,jobName: String,sparkSession: SparkSession): Unit = {
    val df = sparkSession.table(f"$tmpzSchema.${jobName}_tmp_validation")
    df.write.option("partitionOverwriteMode","dynamic").format("delta").mode("overwrite").
      partitionBy("feed_ind","ictrl_dt").saveAsTable(f"$schemaName.$tableName")
  }

  def checkRunningIctrlDt(jobName: String,catchUpType: String,
                          ictrlDtPattern: String,
                          frequency: String,startTime: LocalDateTime,
                          endTime: LocalDateTime,connectionInfo: ConnectionInfo,
                          dagRunId: String,roundTime: LocalDateTime,
                          processJobType: String): Unit = {
    var listICtrlDtQuery: List[String] = List.empty[String]
    if(catchUpType.equals(CATCHUP_TYPE.PERIOD.getValue)) {
      var startTimeTemp: LocalDateTime = startTime
      while(startTimeTemp.isBefore(endTime) || startTimeTemp.equals(endTime)) {
        val startICtrlDt = startTimeTemp.format(DateTimeFormatter.ofPattern(ictrlDtPattern))
        listICtrlDtQuery = listICtrlDtQuery :+ f"'$startICtrlDt'"
        startTimeTemp = addDateByFrequency(startTimeTemp,frequency)
      }
    }
    else {
      val startICtrlDt = startTime.format(DateTimeFormatter.ofPattern(ictrlDtPattern))
      listICtrlDtQuery = listICtrlDtQuery :+ f"'$startICtrlDt'"
    }
    val queryTransLog = f"""
    SELECT job_nm, tasksgroup_nm, round_time, dag_run_id, schema_nm, table_nm, job_start_time, job_end_time, ictrl_dt as ictrl_dt, status
    FROM $schemaName.tbl_trans_audit_logs
    WHERE job_nm = '$jobName' AND ictrl_dt IN (${listICtrlDtQuery.mkString(",")}) AND (status = 'RUNNING' OR status = 'WAITING')
    AND round_time != '$roundTime'
    ORDER BY job_start_time DESC
    LIMIT 1
    """
    val dfResultLog = ConnectionService.postgresqlQueryDirectly(connectionInfo.getIp, connectionInfo.getPort,
      connectionInfo.getDbName, connectionInfo.getUserNm,
      connectionInfo.getPassword,queryTransLog)
    try {
      breakable {
        while (dfResultLog.rs.next()) {
          val lastRoundTimeWIctrlDt = dfResultLog.rs.getTimestamp("job_start_time")
          if(lastRoundTimeWIctrlDt != null) {
            val strFormatRoundTime = new java.text.SimpleDateFormat("yyyyMMdd").format(lastRoundTimeWIctrlDt)
            val strRoundTimeIn = roundTime.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
            if (strFormatRoundTime != strRoundTimeIn) {
              logger.info("Check log Complete, current round_time is newer by 1 day than the last round_time.")
              break()
            }
            else {
              throw new InvalidArgumentException(s"DropDuplicatesJobError: $jobName")
            }
          }
          else {
            throw new InvalidArgumentException(s"DropDuplicatesJobError: $jobName")
          }
        }
      }
    }
    finally{
      dfResultLog.close()
    }
    if (processJobType == "ongoing" && checkFrequencyAndTgtFmt(frequency,ictrlDtPattern)) {
      val queryTransLog = f"""
      SELECT job_nm, tasksgroup_nm, round_time, dag_run_id, schema_nm, table_nm, job_start_time, job_end_time, ictrl_dt as ictrl_dt, status
      FROM $schemaName.tbl_trans_audit_logs
      WHERE job_nm = '$jobName' AND ictrl_dt IN (${listICtrlDtQuery.mkString(",")}) AND status = 'SUCCEED'
      AND round_time != '$roundTime'
      ORDER BY round_time DESC
      LIMIT 1
      """
      val dfResultLog = ConnectionService.postgresqlQueryDirectly(connectionInfo.getIp, connectionInfo.getPort,
        connectionInfo.getDbName, connectionInfo.getUserNm,
        connectionInfo.getPassword,queryTransLog)
      try {
        breakable {
          while (dfResultLog.rs.next()) {
            throw new DropSuccessJobError(s"DropDuplicatesJobError: $jobName")
          }
        }
      }
      finally{
        dfResultLog.close()
      }
    }
  }



  def checkDependencyByJobName(controlJobDf: Row,
                               masterRefDate: LocalDateTime,connectionInfo: ConnectionInfo,
                               refDateIctrlDt: String, startICtrlDt: String,
                               postgresConnectionInfo: ConnectionInfo,
                               jobName: String, schemaMap: mutable.Map[String,String]): java.util.Map[String,Boolean] = {
    val ictrlDtPattern = controlJobDf.getAs[String]("ictrl_dt_tgtfmt")
    val frequencyValue = controlJobDf.getAs[String]("frequency")
    val backdate = controlJobDf.getAs[Any]("back_day")
    val df = ConnectionService.postgresqlQueryDirectly(
      connectionInfo.getIp,connectionInfo.getPort,
      connectionInfo.getDbName,connectionInfo.getUserNm,connectionInfo.getPassword
      ,s"select * from $schemaName.tbl_job_dependency where " +
        s"job_nm = '$jobName'")
    val returnJobMap = new java.util.HashMap[String,Boolean]()
    try {
      while(df.rs.next()) {
        val r = df.rs
        val activeFlag = r.getString("active_flag")
        val preReqSchemaNm = r.getString("prerequisite_schema_nm")
        val uatSchemaVariable = SCHEMA_LIST.schemaUatMap.get(preReqSchemaNm)
        val trueDevSchemaVariable = SCHEMA_LIST.schemaTrueDevMap.get(preReqSchemaNm)
        val prodSchemaVariable = SCHEMA_LIST.schemaMap.get(preReqSchemaNm)
        val trueSandboxSchemaVariable = SCHEMA_LIST.schemaTrueSandboxMap.get(preReqSchemaNm)
        if(uatSchemaVariable != null) {
          schemaMap.put(uatSchemaVariable,preReqSchemaNm)
        }
        if(trueDevSchemaVariable != null) {
          schemaMap.put(trueDevSchemaVariable,preReqSchemaNm)
        }
        if(prodSchemaVariable != null) {
          schemaMap.put(prodSchemaVariable,preReqSchemaNm)
        }
        if(trueSandboxSchemaVariable != null) {
          schemaMap.put(trueSandboxSchemaVariable,preReqSchemaNm)
        }
        if("y".equalsIgnoreCase(activeFlag)) {
          var seqList = checkLog(r.getString("prerequisite_job_nm"),preReqSchemaNm,
            r.getString("prerequisite_table_nm"),r.getString("frequency_check"),
            r.getObject("value"),r.getObject("empty_flag"),refDateIctrlDt,"tbl_trans_audit_logs",
            r.getString("data_column"),convertPythonDateFormatToJava(
              r.getString("ictrl_dt_tgtfmt")),masterRefDate,
            frequencyValue,convertPythonDateFormatToJava(ictrlDtPattern),
            startICtrlDt,backdate,connectionInfo,sparkSession)
          if(!seqList._1) {
            seqList = checkLog(r.getString("prerequisite_job_nm"),preReqSchemaNm,
              r.getString("prerequisite_table_nm"),r.getString("frequency_check"),
              r.getObject("value"),r.getObject("empty_flag"),refDateIctrlDt,"tbl_ingest_audit_logs",
              r.getString("data_column"),convertPythonDateFormatToJava(
                r.getString("ictrl_dt_tgtfmt")),masterRefDate,
              frequencyValue,convertPythonDateFormatToJava(ictrlDtPattern),
              startICtrlDt,backdate,connectionInfo,sparkSession)
          }
          returnJobMap.put(r.getString("prerequisite_job_nm"),seqList._1)
        }
      }
    }
    finally {
      df.close()
    }
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

    logger.info(s"Business column : $businessColumn")
    val tblIngestAuditLogs = s"$schemaName.tbl_ingest_audit_logs"
    val tblTauditLogs = s"$schemaName.tbl_trans_audit_logs"

    val queryDict = Map(
      "tbl_ingest_audit_logs" -> s"SELECT target_table_nm, job_start_time, ictrl_dt, status, row_cnt FROM $tblIngestAuditLogs WHERE upper(job_nm) = upper('{{prerequisite_job_nm}}') AND target_schema_nm = '{{prerequisite_schema}}' AND target_table_nm = '{{prerequisite_table}}' AND ictrl_dt = {{target_date}} ORDER BY job_start_time DESC",
      "tbl_trans_audit_logs" -> s"SELECT table_nm, job_start_time, ictrl_dt, status, row_cnt FROM $tblTauditLogs WHERE upper(job_nm) = upper('{{prerequisite_job_nm}}') AND schema_nm = '{{prerequisite_schema}}' AND table_nm = '{{prerequisite_table}}' AND ictrl_dt = {{target_date}} ORDER BY job_start_time DESC"
    )

    var baseQuery: String = queryDict.getOrElse(logTable, throw new InvalidArgumentException(s"Unknown log table type: $logTable"))

    // Query for multi-day frequencies will use a different template

    val checkInDate = if (businessColumn == null) "logs" else "raw_date"

    logger.info("check date = {}",refDate)

    var targetDateQueryPart: String = ""
    var listDateTarget: List[String] = List.empty[String] // Dates expected to be present
    var records: Seq[Row] = Seq.empty[Row]

    val emptyFlagInt = Try(emptyFlag.toString.toInt).getOrElse(0)
    val valuesInt = Try(values.toString.toInt).getOrElse(0) // Safe conversion

    frequencyCheck match {
      case "daily" | "weekly" | "cur_month" | "prev_month" | "eom" | "day-n" | "hourly" | "eom_curr" =>
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
          case "eom_curr" =>
            targetDateAsDateTime = masterRefDate.withDayOfMonth(
              masterRefDate.toLocalDate.lengthOfMonth())
          case _ => // "daily" case
            targetDateAsDateTime = masterRefDate
        }
        targetDateQueryPart = s"'${targetDateAsDateTime.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}'"
        listDateTarget = List(targetDateAsDateTime.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck)))

        val msg = s"ref_date = $targetDateQueryPart\n" +
          s"query = ${
            baseQuery.replace("{{prerequisite_job_nm}}", prerequisiteJobNm)
              .replace("{{prerequisite_schema}}", prerequisiteSchema)
              .replace("{{prerequisite_table}}", prerequisiteTable)
              .replace("{{target_date}}", targetDateQueryPart)
          }\n"
        println(msg)

        if (checkInDate == "logs") {
          val queryRecordsStr = baseQuery.replace("{{prerequisite_job_nm}}", prerequisiteJobNm)
            .replace("{{prerequisite_schema}}", prerequisiteSchema)
            .replace("{{prerequisite_table}}", prerequisiteTable)
            .replace("{{target_date}}", targetDateQueryPart)
          val records = ConnectionService.postgresqlQueryDirectly(connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName, connectionInfo.getUserNm, connectionInfo.getPassword, queryRecordsStr)
          try {
            checkExistsDataWithOutCheckMiss(records.rs, listDateTarget.head, prerequisiteJobNm, emptyFlagInt, null)
          }
          finally {
            records.close()
          }
        } else if (checkInDate == "raw_date") {
          println("check in date is raw data")
          records = checkBusinessColumn(frequencyCheck, businessColumn, prerequisiteSchema, prerequisiteTable, targetDateAsDateTime.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck)), sparkSession)
          if (records.isEmpty) {
            val msg = "table is no record.\n"
            println(msg)
            (false, Map(prerequisiteJobNm -> listDateTarget))
          } else {
            if (records.nonEmpty) {
              val msg = "table is record.\n"
              println(msg)
              (true, Map(prerequisiteJobNm -> Seq.empty[String])) // Check Pass
            } else { // Should not be reached if records.nonEmpty
              (false, Map(prerequisiteJobNm -> listDateTarget))
            }
          }
        }
        else {
          (false, Map(prerequisiteJobNm -> listDateTarget))
        }

      case "quarter" | "month_to_date" | "hour_to_date" | "daily_period" | "year_to_date" | "current_quarter" | "start_month_to_eom" |
           "start_year_to_current" | "start_month_to_current" | "prev_month_to_eom" =>
        val queryDictMultiDay = Map(
          "tbl_ingest_audit_logs" -> s"SELECT target_table_nm, job_start_time, ictrl_dt, status, row_cnt FROM $tblIngestAuditLogs WHERE upper(job_nm) = upper('{{prerequisite_job_nm}}') AND target_schema_nm = '{{prerequisite_schema}}' AND target_table_nm = '{{prerequisite_table}}' AND ictrl_dt {{target_date}} ORDER BY job_start_time DESC",
          "tbl_trans_audit_logs" -> s"SELECT table_nm, job_start_time, ictrl_dt, status, row_cnt FROM $tblTauditLogs WHERE upper(job_nm) = upper('{{prerequisite_job_nm}}') AND schema_nm = '{{prerequisite_schema}}' AND table_nm = '{{prerequisite_table}}' AND ictrl_dt {{target_date}} ORDER BY job_start_time DESC"
        )
        baseQuery = queryDictMultiDay.getOrElse(logTable, throw new InvalidArgumentException(s"Unknown log table type: $logTable"))

        frequencyCheck match {
          case "prev_month_to_eom" =>
            val startMonth = masterRefDate.withDayOfMonth(1).withHour(0)
            val endMonth = startMonth.withDayOfMonth(startMonth.toLocalDate.lengthOfMonth()).withHour(23)
            val format = DateTimeFormatter.ofPattern(patternIctrlDateCheck)
            var startMonthTemp = startMonth
            while(!startMonthTemp.isAfter(endMonth)) {
              listDateTarget = listDateTarget :+ startMonthTemp.format(format)
              startMonthTemp = addDateByFrequency(startMonthTemp,frequency)
            }
            listDateTarget = listDateTarget.distinct // Drop duplicates

            val useBetweenQuery = checkOrderDatetimeFormat(patternIctrlDateCheck)
            if (useBetweenQuery) {
              targetDateQueryPart = s"BETWEEN '${startMonth.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}' AND '${endMonth.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}'"
            } else {
              targetDateQueryPart = s"IN (${listDateTarget.map(d => s"'$d'").mkString(", ")})"
            }
          case "start_month_to_eom" =>
            val startMonth = masterRefDate.withDayOfMonth(1).withHour(0)
            val endMonth = masterRefDate.withDayOfMonth(masterRefDate.toLocalDate.lengthOfMonth()).withHour(23)
            val format = DateTimeFormatter.ofPattern(patternIctrlDateCheck)
            var startMonthTemp = startMonth
            while(!startMonthTemp.isAfter(endMonth)) {
              listDateTarget = listDateTarget :+ startMonthTemp.format(format)
              startMonthTemp = addDateByFrequency(startMonthTemp,frequency)
            }
            listDateTarget = listDateTarget.distinct // Drop duplicates

            val useBetweenQuery = checkOrderDatetimeFormat(patternIctrlDateCheck)
            if (useBetweenQuery) {
              targetDateQueryPart = s"BETWEEN '${startMonth.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}' AND '${endMonth.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}'"
            } else {
              targetDateQueryPart = s"IN (${listDateTarget.map(d => s"'$d'").mkString(", ")})"
            }
          case "start_year_to_current" =>
            val startYear = masterRefDate.withMonth(1).withDayOfMonth(1).withHour(0)
            val endRange = masterRefDate
            val format = DateTimeFormatter.ofPattern(patternIctrlDateCheck)
            var startYearTemp = startYear
            while(!startYearTemp.isAfter(endRange)) {
              listDateTarget = listDateTarget :+ startYearTemp.format(format)
              startYearTemp = addDateByFrequency(startYearTemp,frequency)
            }
            listDateTarget = listDateTarget.distinct // Drop duplicates

            val useBetweenQuery = checkOrderDatetimeFormat(patternIctrlDateCheck)
            if (useBetweenQuery) {
              targetDateQueryPart = s"BETWEEN '${startYear.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}' AND '${endRange.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}'"
            } else {
              targetDateQueryPart = s"IN (${listDateTarget.map(d => s"'$d'").mkString(", ")})"
            }
          case "start_month_to_current" =>
            val startMonth = masterRefDate.withDayOfMonth(1).withHour(0)
            val currentMonth = masterRefDate
            val format = DateTimeFormatter.ofPattern(patternIctrlDateCheck)
            var startMonthTemp = startMonth
            while(!startMonthTemp.isAfter(currentMonth)) {
              listDateTarget = listDateTarget :+ startMonthTemp.format(format)
              startMonthTemp = addDateByFrequency(startMonthTemp,frequency)
            }
            listDateTarget = listDateTarget.distinct // Drop duplicates

            val useBetweenQuery = checkOrderDatetimeFormat(patternIctrlDateCheck)
            if (useBetweenQuery) {
              targetDateQueryPart = s"BETWEEN '${startMonth.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}' AND '${currentMonth.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}'"
            } else {
              targetDateQueryPart = s"IN (${listDateTarget.map(d => s"'$d'").mkString(", ")})"
            }
          case "quarter" =>
            val monthStartList = List(1, 4, 7, 10)
            if (!(1 <= valuesInt && valuesInt <= 4)) {
              println(s"values not in 1-4 quarter: $values")
              throw new InvalidArgumentException(s"values not in 1-4 quarter: $values")
            }
            val quarterStartMonth = monthStartList(valuesInt - 1)
            logger.info(s"Target Quarter : $valuesInt Month Quarter Start : $quarterStartMonth")

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

          case "current_quarter"=>
            val monthStartList = List(1, 4, 7, 10)
            val dateRun = masterRefDate
            val monthRun = dateRun.getMonthValue
            val currentQuarter = (monthRun - 1) / 3 + 1
            val quarterStartDate = dateRun.withMonth(monthStartList(currentQuarter - 1)).withDayOfMonth(1).withHour(0)
            var tempDate = quarterStartDate
            while(!tempDate.isAfter(dateRun)) {
              listDateTarget = listDateTarget :+ tempDate.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))
              tempDate = if(patternIctrlDateCheck.equals("yyyyMM")) {
                tempDate.plusMonths(1)
              } else if(patternIctrlDateCheck.equals("yyyyMMdd")) {
                tempDate.plusDays(1)
              } else {
                tempDate.plusHours(1)
              }
            }
            listDateTarget = listDateTarget.distinct // Drop duplicates

            val useBetweenQuery = checkOrderDatetimeFormat(patternIctrlDateCheck)
            if (useBetweenQuery) {
              targetDateQueryPart = s"BETWEEN '${quarterStartDate.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}' AND '${dateRun.format(DateTimeFormatter.ofPattern(patternIctrlDateCheck))}'"
            } else {
              targetDateQueryPart = s"IN (${listDateTarget.map(d => s"'$d'").mkString(", ")})"
            }

          case "month_to_date" =>
            val dateRun = masterRefDate
            val dateStart = dateRun.withDayOfMonth(1)
            var dateEnd: LocalDateTime = null
            if(valuesInt == 31) {
              dateEnd = dateRun.withDayOfMonth(dateRun.toLocalDate.lengthOfMonth())
            }
            else {
              dateEnd = dateRun.withDayOfMonth(valuesInt)
            }
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
            val dateRunLocal = parseToLocalDateTime(refDate, patternIctrlDt).get
            val dateStartLocal = parseToLocalDateTime(startIctrlDt, patternIctrlDt).get
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
          val records = ConnectionService.postgresqlQueryDirectly(connectionInfo.getIp,
            connectionInfo.getPort, connectionInfo.getDbName, connectionInfo.getUserNm,
            connectionInfo.getPassword, formattedQuery)
          try {
            checkExistsDataWithCheckMiss(records.rs, listDateTarget, prerequisiteJobNm, emptyFlagInt, null)
          }
          finally{
            records.close()
          }
        } else if (checkInDate.toLowerCase == "raw_date") {
          records = checkBusinessColumn(frequencyCheck, businessColumn, prerequisiteSchema, prerequisiteTable, targetDateQueryPart,sparkSession) // Pass the formatted string directly
          if (records.isEmpty) {
            println("table is no record.\n")
            (false, Map(prerequisiteJobNm -> listDateTarget))
          } else  {
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
          }
        }
        else {
          (false, Map(prerequisiteJobNm -> Seq.empty[String]))
        }

      case "max_date" =>
        val queryDictMultiDay = Map(
          "tbl_ingest_audit_logs" -> s"SELECT target_table_nm, job_start_time, ictrl_dt, status, row_cnt FROM $tblIngestAuditLogs WHERE upper(job_nm) = upper('{{prerequisite_job_nm}}') AND target_schema_nm = '{{prerequisite_schema}}' AND target_table_nm = '{{prerequisite_table}}' {{target_date}} ORDER BY job_start_time DESC",
          "tbl_trans_audit_logs" -> s"SELECT table_nm, job_start_time, ictrl_dt, status, row_cnt FROM $tblTauditLogs WHERE upper(job_nm) = upper('{{prerequisite_job_nm}}') AND schema_nm = '{{prerequisite_schema}}' AND table_nm = '{{prerequisite_table}}' {{target_date}} ORDER BY job_start_time DESC"
        )
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
          val records = ConnectionService.postgresqlQueryDirectly(
            connectionInfo.getIp, connectionInfo.getPort, connectionInfo.getDbName,
            connectionInfo.getUserNm, connectionInfo.getPassword,
            formattedQuery)
          try {
            checkExistsDataWithCheckMissMaxDate(records.rs, dateQueryStr, prerequisiteJobNm, patternIctrlDateCheck, emptyFlagInt, masterRefDate)
          }
          finally {
            records.close()
          }
        } else {
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
          if (records.isEmpty) {
            println("table is no record.\n")
            (false, Map(prerequisiteJobNm -> Seq(dateQueryStr)))
          } else  {
            val listLogDate = records.map(r => parseToLocalDateTime(r.getString(0), patternIctrlDateCheck).get)
            val lastDatetime = listLogDate.maxBy(_.toEpochSecond(java.time.ZoneOffset.UTC)) // Get max date

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
//        if (records.isEmpty) {
//          println("table is no record.\n")
//          (false, Map(prerequisiteJobNm -> Seq(dateQueryStr)))
//        } else if (businessColumn != null) {
//          val listLogDate = records.map(r => LocalDateTime.parse(r.getString(0), DateTimeFormatter.ofPattern(patternIctrlDateCheck)))
//          val lastDatetime = listLogDate.maxBy(_.toEpochSecond(java.time.ZoneOffset.UTC)) // Get max date
//
//          println(s"Last Datetime : $lastDatetime")
//
//          if (lastDatetime.isEqual(masterRefDate) || lastDatetime.isAfter(masterRefDate)) {
//            println("table is ready.\n")
//            (true, Map(prerequisiteJobNm -> Seq.empty[String]))
//          } else {
//            println("table is not ready.\n")
//            (false, Map(prerequisiteJobNm -> Seq(dateQueryStr)))
//          }
//        } else {
//          val successfulRecords = records.filter(r => r.getString(3) == "SUCCEED" && Option(r.get(4)).map(_.toString.toLong).getOrElse(0L) >= emptyFlagInt)
//          if (successfulRecords.isEmpty) {
//            println("table is not ready (no successful records).\n")
//            (false, Map(prerequisiteJobNm -> Seq(dateQueryStr)))
//          } else {
//            val listLogDate = successfulRecords.map(r => LocalDateTime.parse(r.getString(2), DateTimeFormatter.ofPattern(patternIctrlDateCheck)))
//            val lastDatetime = listLogDate.maxBy(_.toEpochSecond(java.time.ZoneOffset.UTC))
//
//            println(s"Last Datetime : $lastDatetime")
//
//            if (lastDatetime.isEqual(masterRefDate) || lastDatetime.isAfter(masterRefDate)) {
//              println("table is ready.\n")
//              (true, Map(prerequisiteJobNm -> Seq.empty[String]))
//            } else {
//              println("table is not ready.\n")
//              (false, Map(prerequisiteJobNm -> Seq(dateQueryStr)))
//            }
//          }
//        }

      case "date_range" =>
        val queryDict = Map(
          "tbl_ingest_audit_logs" -> s"SELECT target_table_nm, job_start_time, ictrl_dt, status, row_cnt FROM $tblIngestAuditLogs WHERE upper(job_nm) = upper('{{prerequisite_job_nm}}') AND target_schema_nm = '{{prerequisite_schema}}' AND target_table_nm = '{{prerequisite_table}}' AND ictrl_dt {{target_date}} ORDER BY job_start_time DESC",
          "tbl_trans_audit_logs" -> s"SELECT table_nm, job_start_time, ictrl_dt, status, row_cnt FROM $tblTauditLogs WHERE upper(job_nm) = upper('{{prerequisite_job_nm}}') AND schema_nm = '{{prerequisite_schema}}' AND table_nm = '{{prerequisite_table}}' AND ictrl_dt {{target_date}} ORDER BY job_start_time DESC"
        )
        baseQuery = queryDict.getOrElse(logTable, throw new InvalidArgumentException(s"Unknown log table type: $logTable"))

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
          val records = ConnectionService.postgresqlQueryDirectly(connectionInfo.getIp,
            connectionInfo.getPort, connectionInfo.getDbName,
            connectionInfo.getUserNm, connectionInfo.getPassword, formattedQuery)
          try {
            checkExistsDataWithCheckMiss(records.rs, listDateTarget, prerequisiteJobNm, emptyFlagInt, null)
          }
          finally {
            records.close()
          }
        } else  {
          records = checkBusinessColumn(frequencyCheck, businessColumn, prerequisiteSchema, prerequisiteTable, targetDateQueryPart,sparkSession)
          if (records.isEmpty) {
            (false, Map(prerequisiteJobNm -> listDateTarget))
          }
          else {
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
          }
        }

      case "date_in_range" =>
        val queryDictMultiDay = Map(
          "tbl_ingest_audit_logs" -> s"SELECT target_table_nm, job_start_time, ictrl_dt, status, row_cnt FROM $tblIngestAuditLogs WHERE upper(job_nm) = upper('{{prerequisite_job_nm}}') AND target_schema_nm = '{{prerequisite_schema}}' AND target_table_nm = '{{prerequisite_table}}' AND ictrl_dt {{target_date}} ORDER BY job_start_time DESC",
          "tbl_trans_audit_logs" -> s"SELECT table_nm, job_start_time, ictrl_dt, status, row_cnt FROM $tblTauditLogs WHERE upper(job_nm) = upper('{{prerequisite_job_nm}}') AND schema_nm = '{{prerequisite_schema}}' AND table_nm = '{{prerequisite_table}}' AND ictrl_dt {{target_date}} ORDER BY job_start_time DESC"
        )
        baseQuery = queryDictMultiDay.getOrElse(logTable, throw new InvalidArgumentException(s"Unknown log table type: $logTable"))

        val dateRun = masterRefDate
        val valuesInt = Try(values.toString.toInt).getOrElse(0)

        // Calculate theoretical start and end dates for the "BETWEEN" query if applicable
        // The individual dates for the IN clause are generated below.
        val dateStartForBetween = minusDateByFrequency(dateRun, frequency, valuesInt)
        val dateEndForBetween = addDateByFrequency(dateRun, frequency, valuesInt)


        // Generate the list of target dates within the range for comparison
        listDateTarget = List.empty[String]

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
          val records = ConnectionService.postgresqlQueryDirectly(connectionInfo.getIp, connectionInfo.getPort,
            connectionInfo.getDbName, connectionInfo.getUserNm,
            connectionInfo.getPassword, formattedQuery)
          try {
            checkExistsDataWithOutCheckMiss(records.rs, listDateTarget.head, prerequisiteJobNm,emptyFlagInt,null)
          }
          finally {
            records.close()
          }
        } else {
          records = checkBusinessColumn(frequencyCheck, businessColumn, prerequisiteSchema, prerequisiteTable, targetDateQueryPart,sparkSession)
          if (records.isEmpty) {
            println("table is no record.\n")
            (false, Map(prerequisiteJobNm -> listDateTarget))
          } else  {
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
