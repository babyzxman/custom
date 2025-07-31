package com.gable.templar.zeus.custom

import com.gable.templar.constant.JobConstant
import com.gable.templar.custom.view.DependencyCheckModel
import com.gable.templar.zeus.controller.model.LoginUser
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.{lang, util}

class IngestFw extends CustomFw {

  override def doRunFramework(dependencyCheckModel: DependencyCheckModel,
                              JOB_TYPE: JobConstant.JOB_TYPE,sparkSession: SparkSession,
                              loginUser: LoginUser): Unit = {
    if(JOB_TYPE == JobConstant.JOB_TYPE.INGEST_DB) {

    }
  }

  def doRunIngestDb(dependencyCheckModel: DependencyCheckModel,
                    sparkSession: SparkSession,loginUser: LoginUser): Unit = {
    val df = sparkSession.sql(s"""select job_nm, tasksgroup_nm, workflow_nm, connector_source, frequency, sequence, ingestion_type, ictrl_dt_srcfmt, last_success_ictrl_dt, back_day, target_schema_nm, target_table_nm, load_type
                        |                                    from ${schemaName}.tbl_api_ingestion
                        |                                    where lower(job_nm) = lower('${dependencyCheckModel.getJobName}') and lower(active_flag) = lower('Y')""".stripMargin)
    df.take(1).foreach(r => {
      val ictrlDtFormat = r.getAs[String]("ictrl_dt_srcfmt")
      val startIctrlDt = dependencyCheckModel.get_bldStartDate().toLocalDateTime.format(DateTimeFormatter.ofPattern(ictrlDtFormat))
      val endIctrlDt = dependencyCheckModel.get_bldEndDate().toLocalDateTime.format(DateTimeFormatter.ofPattern(ictrlDtFormat))
    })
  }

  override def checkDependencyByJobName(dependencyCheckModel: DependencyCheckModel,sparkSession: SparkSession): util.Map[String, Boolean] = ???
}
