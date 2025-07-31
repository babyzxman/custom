package com.gable.templar.custom;

import com.gable.templar.constant.JobConstant;
import com.gable.templar.custom.view.DependencyCheckModel;
import com.gable.templar.heaven.exception.InvalidArgumentException;
import com.gable.templar.heaven.service.custom.DefaultCustomService;
import com.gable.templar.zeus.custom.CustomFw;
import com.gable.templar.zeus.custom.IngestFw;
import com.gable.templar.zeus.custom.TransformFw;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DependencyCheckCustom extends DefaultCustomService<DependencyCheckModel> {

    private final TransformFw transformFw = new TransformFw();

    private final IngestFw ingestFw = new IngestFw();

    @Override
    public Object execute(DependencyCheckModel params) throws Exception {
        transformFw.setSchemaName("fwconfz_uat");
        Map<String,Boolean> results = transformFw.checkDependencyByJobName(params);
        Set<String> notReadyJob = new HashSet<>();
        Set<String> readyJob = new HashSet<>();
        boolean hasNotReadyJob = false;
        for(Map.Entry<String,Boolean> result: results.entrySet()) {
            if(!result.getValue()) {
                notReadyJob.add(result.getKey());
                hasNotReadyJob = true;
            }
            readyJob.add(result.getKey());
        }
        if(hasNotReadyJob) {
            throw new InvalidArgumentException("The dependency job check failed because this job = " + String.join(",",notReadyJob) + " is not finished");
        }
        return "Dependency check success for job " + String.join(",",readyJob);
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

    @Override
    public DependencyCheckModel createParams() {
        return new DependencyCheckModel();
    }
}
