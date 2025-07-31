package com.gable.templar.custom.view;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Map;

public class NotebookIdAddParameterRequest {

    private String notebookName;
    private String notebookId;
    private String jobName;
    private String parentNotebookId;
    private Map<String, Map<String, JsonNode>> addParameterMapFromTitle;
    public void setNotebookName(String notebookName) {
        this.notebookName = notebookName;
    }

    public String getNotebookName() {
        return notebookName;
    }
    private Map<String, String> specificInterpreterGroupName;

    public void setNotebookId(String notebookId) {
        this.notebookId = notebookId;
    }

    public String getNotebookId() {
        return notebookId;
    }

    public void setAddParameterMapFromTitle(Map<String, Map<String, JsonNode>> addParameterMapFromTitle) {
        this.addParameterMapFromTitle = addParameterMapFromTitle;
    }

    public Map<String, Map<String, JsonNode>> getAddParameterMapFromTitle() {
        return addParameterMapFromTitle;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getJobName() {
        return jobName;
    }

    public void setParentNotebookId(String parentNotebookId) {
        this.parentNotebookId = parentNotebookId;
    }

    public String getParentNotebookId() {
        return parentNotebookId;
    }
    public void setSpecificInterpreterGroupName(Map<String, String> specificInterpreterGroupName) {
        this.specificInterpreterGroupName = specificInterpreterGroupName;
    }

    public Map<String, String> getSpecificInterpreterGroupName() {
        return specificInterpreterGroupName;
    }
}
