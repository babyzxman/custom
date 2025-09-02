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
import com.gable.templar.zeus.custom.*;
import com.gable.templar.zeus.service.vector.ConnectionInfo;
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

    private TransformFw transformFw;

    private IngestFw ingestFw;

    private final GeneralService generalService = new GeneralService();

    @PostConstruct
    void init() {
        transformFw = new TransformFw("fwconfz_uat",
                heraConfig.getHeraUrl(),loginUser, SparkServer.getZeusSession().session(),
                NewThreadExecutor.threadExecutor);
        ingestFw =  new IngestFw("fwconfz_uat",
                heraConfig.getHeraUrl(),loginUser, SparkServer.getZeusSession().session(),
                NewThreadExecutor.threadExecutor);
    }

    @Override
    public Object execute(DependencyCheckModel params) throws Exception {
        return generalService.doRunFrameWork(params,transformFw,ingestFw,"fwconfz_uat");
    }

    @Override
    public DependencyCheckModel createParams() {
        return new DependencyCheckModel();
    }
}
