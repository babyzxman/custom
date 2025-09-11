package com.gable.templar.custom.view;

import java.util.List;

public class RunParallelResult {

    private List<String> successList;

    private List<String> failedList;

    public List<String> getSuccessList() {
        return successList;
    }

    public void setSuccessList(List<String> successList) {
        this.successList = successList;
    }

    public List<String> getFailedList() {
        return failedList;
    }

    public void setFailedList(List<String> failedList) {
        this.failedList = failedList;
    }
}
