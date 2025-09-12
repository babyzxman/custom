package com.gable.templar.custom.view;

import com.gable.templar.constant.JobConstant;
import org.apache.spark.sql.Row;

import java.util.List;

public class SequenceJobInformation {

    private String tblConf;

    private JobConstant.JOB_TYPE jobType;

    private Row[] data;

    public String getTblConf() {
        return tblConf;
    }

    public void setTblConf(String tblConf) {
        this.tblConf = tblConf;
    }

    public JobConstant.JOB_TYPE getJobType() {
        return jobType;
    }

    public void setJobType(JobConstant.JOB_TYPE jobType) {
        this.jobType = jobType;
    }

    public Row[] getData() {
        return data;
    }

    public void setData(Row[] data) {
        this.data = data;
    }
}
