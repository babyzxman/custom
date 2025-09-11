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
import com.gable.templar.zeus.service.spark.SparkHiveMetaStoreService;
import com.gable.templar.zeus.service.spark.TableManageService;
import com.gable.templar.zeus.service.vector.ConnectionInfo;
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

    private TransformFw transformFw;

    private IngestFw ingestFw;

    @Autowired
    private TableManageService tableManageService;

    @Autowired
    private SparkHiveMetaStoreService sparkHiveMetaStoreService;

    @PostConstruct
    void init() {
        transformFw = new TransformFw("fwconfz",
                heraConfig.getHeraUrl(),loginUser, SparkServer.getZeusSession().session(),
                NewThreadExecutor.threadExecutor,tableManageService,sparkHiveMetaStoreService);
        ingestFw =  new IngestFw("fwconfz",
                heraConfig.getHeraUrl(),loginUser, SparkServer.getZeusSession().session(),
                NewThreadExecutor.threadExecutor);
    }

    private final GeneralService generalService = new GeneralService();

    @Override
    public Object execute(DependencyCheckModel params) throws Exception {
        return generalService.doRunFrameWork(params,transformFw,ingestFw,"fwconfz");
    }

    @Override
    public DependencyCheckModel createParams() {
        return new DependencyCheckModel();
    }
}
