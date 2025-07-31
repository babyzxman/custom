package com.gable.templar.zeus.custom

import com.gable.templar.constant.JobConstant
import com.gable.templar.constant.JobConstant.JOB_TYPE
import com.gable.templar.heaven.exception.InvalidArgumentException
import org.apache.spark.sql.SparkSession

class GeneralService {



  def checkJobTypeFromJobName(jobName: String,sparkSession: SparkSession,schemaName: String): JOB_TYPE = {
    if(!sparkSession.sql(f"select * from ${schemaName}.tbl_job_trans where job_nm = '${jobName}'").isEmpty) {
      JOB_TYPE.TRANSFORM
    }
    else if(!sparkSession.sql(f"select * from ${schemaName}.${JobConstant.tableNmApiIngestion} where job_nm = '${jobName}'").isEmpty) {
      JOB_TYPE.INGEST_API
    }
    else if(!sparkSession.sql(f"select * from ${schemaName}.${JobConstant.tableNmDbIngestion} where job_nm = '${jobName}'").isEmpty) {
      JOB_TYPE.INGEST_DB
    }
    else {
      throw new InvalidArgumentException("The config job name is not in TBL_CONFIG")
    }
  }

}
