package com.gable.templar.zeus.custom

import com.gable.templar.constant.JobConstant
import com.gable.templar.constant.JobConstant.JOB_TYPE
import com.gable.templar.custom.view.{DependencyCheckModel, ExecuteResponse, ExecuteResponseWrap}
import com.gable.templar.heaven.exception.InvalidArgumentException
import com.gable.templar.zeus.SparkServer
import com.gable.templar.zeus.service.vector.ConnectionInfo
import org.apache.spark.sql.SparkSession

import java.util

class GeneralService {

  private val salt = "rTYlPkZH37QOf7Xx1GzZ0hakdl/2/Z02HlPesDfQ2lM="

  private val ultKey = "AdKX67Zn0JRJSGJQ7/4LrQOsZ0IW8+Fcdh7hpeJV8GeVNiPIs4i0RZ4T+XjXyEb0"

  def getCustomFwClassByJobType(jobType: JobConstant.JOB_TYPE,transformFw: CustomFw,ingestFw: CustomFw): CustomFw = {
    jobType match {
      case JOB_TYPE.OUTBOUND | JOB_TYPE.TRANSFORM =>
        transformFw
      case JOB_TYPE.FILE | JOB_TYPE.KAFKA | JOB_TYPE.INGEST_API | JOB_TYPE.INGEST_DB =>
        ingestFw
    }
  }

  def doRunFrameWork(params: DependencyCheckModel,
                     transformFw: CustomFw,ingestFw: IngestFw,
                     schemaName: String): ExecuteResponseWrap = {
    val sparkSession = SparkServer.getZeusSession.session
    var jobType: JobConstant.JOB_TYPE = null
    val queryMasterSql = s"SELECT system,key,values FROM $schemaName.tbl_master_config where system = 'fw_postgre'"
    val postgresConnectionInfo = ConnectionService.getMasterConfigLog(queryMasterSql, salt, ultKey)
    val executeResponseWrap = new ExecuteResponseWrap
    if(params.getJobNames == null || params.getJobNames.isEmpty) {
      if (params.getTaskGroupName != null) jobType = checkJobTypeFromTaskGroup(params.getTaskGroupName, sparkSession, schemaName)
      else jobType = checkJobTypeFromJobName(params.getJobName, schemaName, postgresConnectionInfo)
      val customFw = getCustomFwClassByJobType(jobType,transformFw,ingestFw)
      executeResponseWrap.setExecuteResponseList(customFw.doRunTaskGroup(params, jobType))
    }
    else {
      val executeResponseList: java.util.ArrayList[ExecuteResponse] = new util.ArrayList[ExecuteResponse]()
      params.getJobNames.forEach(j => {
        jobType = checkJobTypeFromJobName(j, schemaName, postgresConnectionInfo)
        val customFw = getCustomFwClassByJobType(jobType,transformFw,ingestFw)
        executeResponseList.addAll(customFw.doRunTaskGroup(params, jobType))
      })
      executeResponseWrap.setExecuteResponseList(executeResponseList)
    }
    executeResponseWrap
  }

  def checkJobTypeFromTaskGroup(taskGroupName: String,sparkSession: SparkSession, schemaName: String): JOB_TYPE = {
    if(!sparkSession.sql(f"select * from ${schemaName}.tbl_job_trans where tasksgroup_nm = '${taskGroupName}'").isEmpty) {
      JOB_TYPE.TRANSFORM
    }
    else if(!sparkSession.sql(f"select * from ${schemaName}.tbl_job_outbound where tasksgroup_nm = '${taskGroupName}'").isEmpty) {
      JOB_TYPE.OUTBOUND
    }
    else if(!sparkSession.sql(f"select * from ${schemaName}.${JobConstant.tableNmApiIngestion} where tasksgroup_nm = '${taskGroupName}'").isEmpty) {
      JOB_TYPE.INGEST_API
    }
    else if(!sparkSession.sql(f"select * from ${schemaName}.${JobConstant.tableNmDbIngestion} where tasksgroup_nm = '${taskGroupName}'").isEmpty) {
      JOB_TYPE.INGEST_DB
    }
    else if(!sparkSession.sql(f"select * from ${schemaName}.${JobConstant.tableNmFileIngestion} where tasksgroup_nm = '${taskGroupName}'").isEmpty) {
      JOB_TYPE.FILE
    }
    else if (!sparkSession.sql(f"select * from $schemaName.${JobConstant.tableNmKafkaIngestion} where tasksgroup_nm = '${taskGroupName}'").isEmpty) {
      JOB_TYPE.KAFKA
    }
    else {
      throw new InvalidArgumentException("The config job name is not in TBL_CONFIG")
    }
  }

  def checkJobTypeFromJobName(jobName: String,schemaName: String,postgresConnection: ConnectionInfo): JOB_TYPE = {
    var sql = f"select * from ${schemaName}.tbl_job_trans where job_nm = '${jobName}'"
    var df = ConnectionService.postgresqlQueryDirectly(postgresConnection.getIp,
      postgresConnection.getPort, postgresConnection.getDbName,
      postgresConnection.getUserNm,postgresConnection.getPassword,sql)
    try {
      while (df.rs.next()) {
        return JOB_TYPE.TRANSFORM
      }
    }
    finally {
      df.close()
    }
    sql = f"select * from ${schemaName}.tbl_job_outbound where job_nm = '${jobName}'"
    df = ConnectionService.postgresqlQueryDirectly(postgresConnection.getIp,
      postgresConnection.getPort, postgresConnection.getDbName,
      postgresConnection.getUserNm,postgresConnection.getPassword,sql)
    try {
      while (df.rs.next()) {
        return JOB_TYPE.OUTBOUND
      }
    }
    finally {
      df.close()
    }
    sql = f"select * from ${schemaName}.${JobConstant.tableNmApiIngestion} where job_nm = '${jobName}'"
    df = ConnectionService.postgresqlQueryDirectly(postgresConnection.getIp,
      postgresConnection.getPort, postgresConnection.getDbName,
      postgresConnection.getUserNm,postgresConnection.getPassword,sql)
    try {
      while (df.rs.next()) {
        return JOB_TYPE.INGEST_API
      }
    }
    finally {
      df.close()
    }
    sql = f"select * from ${schemaName}.${JobConstant.tableNmDbIngestion} where job_nm = '${jobName}'"
    df = ConnectionService.postgresqlQueryDirectly(postgresConnection.getIp,
      postgresConnection.getPort, postgresConnection.getDbName,
      postgresConnection.getUserNm,postgresConnection.getPassword,sql)
    try {
      while (df.rs.next()) {
        return JOB_TYPE.INGEST_DB
      }
    }
    finally {
      df.close()
    }
    sql = f"select * from ${schemaName}.${JobConstant.tableNmFileIngestion} where job_nm = '${jobName}'"
    df = ConnectionService.postgresqlQueryDirectly(postgresConnection.getIp,
      postgresConnection.getPort, postgresConnection.getDbName,
      postgresConnection.getUserNm,postgresConnection.getPassword,sql)
    try {
      while (df.rs.next()) {
        return JOB_TYPE.FILE
      }
    }
    finally {
      df.close()
    }
    sql = f"select * from ${schemaName}.${JobConstant.tableNmKafkaIngestion} where job_nm = '${jobName}'"
    df = ConnectionService.postgresqlQueryDirectly(postgresConnection.getIp,
      postgresConnection.getPort, postgresConnection.getDbName,
      postgresConnection.getUserNm,postgresConnection.getPassword,sql)
    try {
      while (df.rs.next()) {
        return JOB_TYPE.KAFKA
      }
    }
    finally {
      df.close()
    }
    throw new InvalidArgumentException("The config job name is not in TBL_CONFIG")
  }

  def checkJobTypeFromJobName(jobName: String,sparkSession: SparkSession,schemaName: String): JOB_TYPE = {
    if(!sparkSession.sql(f"select * from ${schemaName}.tbl_job_trans where job_nm = '${jobName}'").isEmpty) {
      JOB_TYPE.TRANSFORM
    }
    else if(!sparkSession.sql(f"select * from ${schemaName}.tbl_job_outbound where job_nm = '${jobName}'").isEmpty) {
      JOB_TYPE.OUTBOUND
    }
    else if(!sparkSession.sql(f"select * from ${schemaName}.${JobConstant.tableNmApiIngestion} where job_nm = '${jobName}'").isEmpty) {
      JOB_TYPE.INGEST_API
    }
    else if(!sparkSession.sql(f"select * from ${schemaName}.${JobConstant.tableNmDbIngestion} where job_nm = '${jobName}'").isEmpty) {
      JOB_TYPE.INGEST_DB
    }
    else if(!sparkSession.sql(f"select * from ${schemaName}.${JobConstant.tableNmFileIngestion} where job_nm = '${jobName}'").isEmpty) {
      JOB_TYPE.FILE
    }
    else if (!sparkSession.sql(f"select * from $schemaName.${JobConstant.tableNmKafkaIngestion} where job_nm = '${jobName}'").isEmpty) {
      JOB_TYPE.KAFKA
    }
    else {
      throw new InvalidArgumentException("The config job name is not in TBL_CONFIG")
    }
  }

}
