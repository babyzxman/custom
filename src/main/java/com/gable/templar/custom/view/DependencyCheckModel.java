package com.gable.templar.custom.view;

import com.fasterxml.jackson.databind.JsonNode;

import java.sql.Timestamp;
import java.util.List;

public class DependencyCheckModel {

    private String jobName;

    private List<String> jobNames;

    private String fixedDate;

    private String taskGroupName;

    private String _workflowId;

    private String _runId;

    private String _taskId;

    private Timestamp _bldStartDate;

    private Timestamp _bldEndDate;

    private String runType;

    private String workspaceName;

    private Long timeout = 86400L;

    private Long timeSleep = 10L;

    private String moduleNotebookName;

    private Boolean runInTimeRange = false;

    private String notebookId;

    private JsonNode sparkConf;

    public String getRunType() {
        return runType;
    }

    public void setRunType(String runType) {
        this.runType = runType;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public Long getTimeSleep() {
        return timeSleep;
    }

    public void setTimeSleep(Long timeSleep) {
        this.timeSleep = timeSleep;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getFixedDate() {
        return fixedDate;
    }

    public void setFixedDate(String fixedDate) {
        this.fixedDate = fixedDate;
    }

    public String getTaskGroupName() {
        return taskGroupName;
    }

    public void setTaskGroupName(String taskGroupName) {
        this.taskGroupName = taskGroupName;
    }

    public String get_workflowId() {
        return _workflowId;
    }

    public void set_workflowId(String _workflowId) {
        this._workflowId = _workflowId;
    }

    public String get_runId() {
        return _runId;
    }

    public void set_runId(String _runId) {
        this._runId = _runId;
    }

    public String get_taskId() {
        return _taskId;
    }

    public void set_taskId(String _taskId) {
        this._taskId = _taskId;
    }

    public Timestamp get_bldStartDate() {
        return _bldStartDate;
    }

    public void set_bldStartDate(Timestamp _bldStartDate) {
        this._bldStartDate = _bldStartDate;
    }

    public Timestamp get_bldEndDate() {
        return _bldEndDate;
    }

    public void set_bldEndDate(Timestamp _bldEndDate) {
        this._bldEndDate = _bldEndDate;
    }

    public String getModuleNotebookName() {
        return moduleNotebookName;
    }

    public void setModuleNotebookName(String moduleNotebookName) {
        this.moduleNotebookName = moduleNotebookName;
    }

    public String getNotebookId() {
        return notebookId;
    }

    public void setNotebookId(String notebookId) {
        this.notebookId = notebookId;
    }

    public Boolean getRunInTimeRange() {
        return runInTimeRange;
    }

    public void setRunInTimeRange(Boolean runInTimeRange) {
        this.runInTimeRange = runInTimeRange;
    }

    public String getWorkspaceName() {
        return workspaceName;
    }

    public void setWorkspaceName(String workspaceName) {
        this.workspaceName = workspaceName;
    }

    public List<String> getJobNames() {
        return jobNames;
    }

    public void setJobNames(List<String> jobNames) {
        this.jobNames = jobNames;
    }

    public JsonNode getSparkConf() {
        return sparkConf;
    }

    public void setSparkConf(JsonNode sparkConf) {
        this.sparkConf = sparkConf;
    }
}
