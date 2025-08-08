package com.gable.templar.custom.view;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class AirflowNotebookRunParallelRequest {

    private Integer concurrentParallel = 1;

    private int timeSleep = 10;

    private Integer timeout = 86400;

    private String language;

    private String moduleNotebookName;

    private Long moduleNotebookId;

    private Boolean workflowRun = false;

    private List<NotebookIdAddParameterRequest> notebookAddParameterRequest;

    private Boolean description;

    private String workspaceId;

    private String workspaceName;

    private boolean isAsync = true;


    public void setConcurrentParallel(Integer concurrentParallel) {
        this.concurrentParallel = concurrentParallel;
    }

    public Integer getConcurrentParallel() {
        return concurrentParallel;
    }

    public void setTimeSleep(int timeSleep) {
        this.timeSleep = timeSleep;
    }

    public int getTimeSleep() {
        return timeSleep;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setNotebookAddParameterRequest(List<NotebookIdAddParameterRequest> notebookAddParameterRequest) {
        this.notebookAddParameterRequest = notebookAddParameterRequest;
    }

    public List<NotebookIdAddParameterRequest> getNotebookAddParameterRequest() {
        return notebookAddParameterRequest;
    }

    public Boolean getWorkflowRun() {
        return workflowRun;
    }

    public void setWorkflowRun(Boolean workflowRun) {
        this.workflowRun = workflowRun;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getLanguage() {
        return language;
    }

    public void setModuleNotebookId(Long moduleNotebookId) {
        this.moduleNotebookId = moduleNotebookId;
    }

    public Long getModuleNotebookId() {
        return moduleNotebookId;
    }

    public void setModuleNotebookName(String moduleNotebookName) {
        this.moduleNotebookName = moduleNotebookName;
    }

    public String getModuleNotebookName() {
        return moduleNotebookName;
    }

    public void setDescription(Boolean description) {
        this.description = description;
    }

    public Boolean getDescription() {
        return description;
    }

    public void setWorkspaceName(String workspaceName) {
        this.workspaceName = workspaceName;
    }

    public String getWorkspaceName() {
        return workspaceName;
    }

    public void setWorkspaceId(String workspaceId) {
        this.workspaceId = workspaceId;
    }

    public String getWorkspaceId() {
        return workspaceId;
    }

    @JsonProperty("isAsync")
    public boolean isAsync() {
        return isAsync;
    }

    @JsonProperty("isAsync")
    public void setAsync(boolean async) {
        isAsync = async;
    }
}
