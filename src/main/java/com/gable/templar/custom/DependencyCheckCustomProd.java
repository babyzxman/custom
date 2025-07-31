package com.gable.templar.custom;

import com.gable.templar.constant.JobConstant;
import com.gable.templar.custom.view.DependencyCheckModel;
import com.gable.templar.heaven.exception.InvalidArgumentException;
import com.gable.templar.heaven.service.custom.DefaultCustomService;
import com.gable.templar.zeus.SparkServer;
import com.gable.templar.zeus.controller.model.LoginUser;
import com.gable.templar.zeus.custom.CustomFw;
import com.gable.templar.zeus.custom.GeneralService;
import com.gable.templar.zeus.custom.IngestFw;
import com.gable.templar.zeus.custom.TransformFw;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DependencyCheckCustomProd extends DefaultCustomService<DependencyCheckModel> {

    @Autowired
    private LoginUser loginUser;

    private final TransformFw transformFw = new TransformFw();

    private final IngestFw ingestFw = new IngestFw();

    private final GeneralService generalService = new GeneralService();

    @Override
    public Object execute(DependencyCheckModel params) throws Exception {
        SparkSession sparkSession = SparkServer.getZeusSession().session();
        JobConstant.JOB_TYPE jobType = generalService.checkJobTypeFromJobName(
                params.getJobName(),sparkSession,"fwconfz");
        CustomFw customFw = getCustomFwClassByJobType(jobType);
        customFw.setSchemaName("fwconfz");
        customFw.doRunFramework(params,jobType,sparkSession,loginUser);
        return "Dependency check success for job " + String.join(",",readyJob);
    }

    @Override
    public DependencyCheckModel createParams() {
        return new DependencyCheckModel();
    }

    public CustomFw getCustomFwClassByJobType(JobConstant.JOB_TYPE jobType) {
        switch (jobType) {
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
