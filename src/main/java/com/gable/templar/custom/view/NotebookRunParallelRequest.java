package com.gable.templar.custom.view;

public class NotebookRunParallelRequest extends AirflowNotebookRunParallelRequest {

    private String runBy;

    private String runningId;

    public void setRunBy(String runBy) {
        this.runBy = runBy;
    }

    public String getRunBy() {
        return runBy;
    }


    public void setRunningId(String runningId) {
        this.runningId = runningId;
    }

    public String getRunningId() {
        return runningId;
    }
}
