package com.gable.templar.custom;

import com.gable.templar.custom.view.DependencyCheckModel;
import com.gable.templar.custom.view.ExecuteResponse;
import com.gable.templar.custom.view.NotebookRunParallelResponse;
import com.gable.templar.custom.view.RunNotebookParallelResult;
import com.gable.templar.heaven.exception.InvalidArgumentException;
import com.gable.templar.heaven.service.custom.DefaultCustomService;
import com.gable.templar.zeus.SparkServer;
import com.gable.templar.zeus.config.HeraConfig;
import com.gable.templar.zeus.controller.model.LoginUser;
import com.gable.templar.zeus.custom.GeneralService;
import com.gable.templar.zeus.custom.IngestFw;
import com.gable.templar.zeus.custom.TransformFw;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

public class RunNotebookParallel extends DefaultCustomService<DependencyCheckModel> {


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
//        String runId = "";
//        if(params.get_workflowId() != null) {
//            runId = params.get_workflowId() + "||" + params.get_runId() + "||" + params.get_taskId();
//        }
//        else {
//            runId = "manual_run_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HH_mm_ss"));
//        }
//
//        RunNotebookParallelResult runNotebookParallelResult =
//                ingestFw.doRunNotebookParallel(new HashMap<>(),
//                        params.getNotebookId(),params,loginUser,runId,);
//        if(runNotebookParallelResult.getErrorMsg() != null) {
//            throw new InvalidArgumentException(runNotebookParallelResult.getErrorMsg());
//        }
//        else {
//            ExecuteResponse executeResponse = new ExecuteResponse();
//            executeResponse.setMessage(runNotebookParallelResult.getMessage());
//            return executeResponse;
//        }
        return null;
    }

    @Override
    public DependencyCheckModel createParams() {
        return new DependencyCheckModel();
    }
}
