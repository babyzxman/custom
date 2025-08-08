package com.gable.templar.custom.view;

public class RunNotebookParallelResult {

    private String message;

    private String errorMsg;

    private String notebookUrl;

    private String runId;

    private String errorSpecificMsg;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public String getNotebookUrl() {
        return notebookUrl;
    }

    public void setNotebookUrl(String notebookUrl) {
        this.notebookUrl = notebookUrl;
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public String getErrorSpecificMsg() {
        return errorSpecificMsg;
    }

    public void setErrorSpecificMsg(String errorSpecificMsg) {
        this.errorSpecificMsg = errorSpecificMsg;
    }
}
