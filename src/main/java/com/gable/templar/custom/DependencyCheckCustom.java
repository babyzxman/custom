package com.gable.templar.custom;

import com.gable.templar.constant.JobConstant;
import com.gable.templar.custom.view.DependencyCheckModel;
import com.gable.templar.custom.view.ExecuteResponse;
import com.gable.templar.custom.view.ExecuteResponseWrap;
import com.gable.templar.heaven.exception.InvalidArgumentException;
import com.gable.templar.heaven.service.custom.DefaultCustomService;
import com.gable.templar.zeus.SparkServer;
import com.gable.templar.zeus.config.HeraConfig;
import com.gable.templar.zeus.controller.model.LoginUser;
import com.gable.templar.zeus.custom.CustomFw;
import com.gable.templar.zeus.custom.GeneralService;
import com.gable.templar.zeus.custom.IngestFw;
import com.gable.templar.zeus.custom.TransformFw;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DependencyCheckCustom extends DefaultCustomService<DependencyCheckModel> {

    @Autowired
    private HeraConfig heraConfig;

    @Autowired
    private LoginUser loginUser;

    @Autowired
    private TaskExecutor taskExecutor;

    private TransformFw transformFw;

    private IngestFw ingestFw;

    private final GeneralService generalService = new GeneralService();

    @PostConstruct
    void init() {
        transformFw = new TransformFw("fwconfz_uat",
                heraConfig.getHeraUrl(),loginUser, SparkServer.getZeusSession().session(),
                taskExecutor);
        ingestFw =  new IngestFw("fwconfz_uat",
                heraConfig.getHeraUrl(),loginUser, SparkServer.getZeusSession().session(),
                taskExecutor);
    }

    @Override
    public Object execute(DependencyCheckModel params) throws Exception {
        SparkSession sparkSession = SparkServer.getZeusSession().session();
        JobConstant.JOB_TYPE jobType = null;
        if(params.getTaskGroupName() != null) {
            jobType = generalService.checkJobTypeFromTaskGroup(
                    params.getTaskGroupName(),sparkSession,"fwconfz_uat");
        }
        else {
            jobType = generalService.checkJobTypeFromJobName(
                    params.getJobName(), sparkSession, "fwconfz_uat");
        }
        CustomFw customFw = getCustomFwClassByJobType(jobType);
        ExecuteResponseWrap executeResponseWrap = new ExecuteResponseWrap();
        executeResponseWrap.setExecuteResponseList(customFw.doRunTaskGroup(params,jobType));
        return executeResponseWrap;
    }

    public CustomFw getCustomFwClassByJobType(JobConstant.JOB_TYPE jobType) {
        switch (jobType) {
            case OUTBOUND:
            case TRANSFORM: {
                return transformFw;
            }
            case INGEST_API:
            case KAFKA:
            case FILE:
            case INGEST_DB: {
                return ingestFw;
            }
        }
        return null;
    }

    @Override
    public DependencyCheckModel createParams() {
        return new DependencyCheckModel();
    }
}
