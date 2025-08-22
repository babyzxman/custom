package com.gable.templar.zeus.custom

import com.gable.templar.constant.JobConstant
import com.gable.templar.constant.JobConstant.JOB_TYPE
import com.gable.templar.heaven.exception.InvalidArgumentException
import com.gable.templar.zeus.service.vector.ConnectionInfo
import org.apache.spark.sql.SparkSession

class GeneralService {

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
