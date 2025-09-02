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

    private TransformFw transformFw;

    private IngestFw ingestFw;

    private final GeneralService generalService = new GeneralService();

    @PostConstruct
    void init() {
        transformFw = new TransformFw("fwconfz_true_dev",
                heraConfig.getHeraUrl(),loginUser, SparkServer.getZeusSession().session(),
                NewThreadExecutor.threadExecutor);
        ingestFw =  new IngestFw("fwconfz_true_dev",
                heraConfig.getHeraUrl(),loginUser, SparkServer.getZeusSession().session(),
                NewThreadExecutor.threadExecutor);
    }


    @Override
    public Object execute(DependencyCheckModel params) throws Exception {
        return generalService.doRunFrameWork(params,transformFw,ingestFw,"fwconfz_true_dev");
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
