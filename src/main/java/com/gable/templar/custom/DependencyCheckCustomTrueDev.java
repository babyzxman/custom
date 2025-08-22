package com.gable.templar.custom;

import com.gable.templar.constant.JobConstant;
import com.gable.templar.custom.view.DependencyCheckModel;
import com.gable.templar.custom.view.ExecuteResponseWrap;
import com.gable.templar.heaven.service.custom.DefaultCustomService;
import com.gable.templar.zeus.SparkServer;
import com.gable.templar.zeus.config.HeraConfig;
import com.gable.templar.zeus.controller.model.LoginUser;
import com.gable.templar.zeus.custom.*;
import com.gable.templar.zeus.service.vector.ConnectionInfo;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;

import javax.annotation.PostConstruct;

public class DependencyCheckCustomTrueDev  extends DefaultCustomService<DependencyCheckModel> {

    @Autowired
    private LoginUser loginUser;

    @Autowired
    private HeraConfig heraConfig;

    @Autowired
    private TaskExecutor taskExecutor;

    private final String salt = "rTYlPkZH37QOf7Xx1GzZ0hakdl/2/Z02HlPesDfQ2lM=";

    private final String ultKey = "AdKX67Zn0JRJSGJQ7/4LrQOsZ0IW8+Fcdh7hpeJV8GeVNiPIs4i0RZ4T+XjXyEb0";

    private TransformFw transformFw;

    private IngestFw ingestFw;

    private final GeneralService generalService = new GeneralService();

    @PostConstruct
    void init() {
        transformFw = new TransformFw("fwconfz_truedev",
                heraConfig.getHeraUrl(),loginUser, SparkServer.getZeusSession().session(),
                taskExecutor);
        ingestFw =  new IngestFw("fwconfz_truedev",
                heraConfig.getHeraUrl(),loginUser, SparkServer.getZeusSession().session(),
                taskExecutor);
    }


    @Override
    public Object execute(DependencyCheckModel params) throws Exception {
        SparkSession sparkSession = SparkServer.getZeusSession().session();
        JobConstant.JOB_TYPE jobType = null;
        String queryMasterSql = "SELECT system,key,values FROM fwconfz_truedev.tbl_master_config where system = 'fw_postgre'";
        ConnectionInfo postgresConnectionInfo = ConnectionService.getMasterConfigLog(queryMasterSql, salt, ultKey);
        if(params.getTaskGroupName() != null) {
            jobType = generalService.checkJobTypeFromTaskGroup(
                    params.getTaskGroupName(),sparkSession,"fwconfz_truedev");
        }
        else {
            jobType = generalService.checkJobTypeFromJobName(
                    params.getJobName(), "fwconfz_truedev",postgresConnectionInfo);
        }
        CustomFw customFw = getCustomFwClassByJobType(jobType);
        ExecuteResponseWrap executeResponseWrap = new ExecuteResponseWrap();
        executeResponseWrap.setExecuteResponseList(customFw.doRunTaskGroup(params,jobType));
        return executeResponseWrap;
    }

    @Override
    public DependencyCheckModel createParams() {
        return new DependencyCheckModel();
    }

    public CustomFw getCustomFwClassByJobType(JobConstant.JOB_TYPE jobType) {
        switch (jobType) {
            case OUTBOUND:
            case TRANSFORM: {
                return transformFw;
            }
            case INGEST_API:
            case INGEST_DB: {
                return ingestFw;
            }
        }
        return null;
    }
}
