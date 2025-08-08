package com.gable.templar.custom;

import com.gable.templar.constant.JobConstant;
import com.gable.templar.custom.view.DependencyCheckModel;
import com.gable.templar.custom.view.ExecuteResponse;
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

import javax.annotation.PostConstruct;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DependencyCheckCustomProd extends DefaultCustomService<DependencyCheckModel> {

    @Autowired
    private LoginUser loginUser;

    @Autowired
    private HeraConfig heraConfig;

    @Autowired
    private TaskExecutor taskExecutor;

    @PostConstruct
    void init() {
        transformFw = new TransformFw("fwconfz",
                heraConfig.getHeraUrl(),loginUser, SparkServer.getZeusSession().session(),
                taskExecutor);
        ingestFw =  new IngestFw("fwconfz",
                heraConfig.getHeraUrl(),loginUser, SparkServer.getZeusSession().session(),
                taskExecutor);
    }

    private TransformFw transformFw;

    private IngestFw ingestFw;

    private final GeneralService generalService = new GeneralService();

    @Override
    public Object execute(DependencyCheckModel params) throws Exception {
        SparkSession sparkSession = SparkServer.getZeusSession().session();
        JobConstant.JOB_TYPE jobType = null;
        if(params.getTaskGroupName() != null) {
            jobType = generalService.checkJobTypeFromTaskGroup(
                    params.getTaskGroupName(),sparkSession,"fwconfz");
        }
        else {
            jobType = generalService.checkJobTypeFromJobName(
                    params.getJobName(), sparkSession, "fwconfz");
        }
        CustomFw customFw = getCustomFwClassByJobType(jobType);
        return customFw.doRunTaskGroup(params,jobType);
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
