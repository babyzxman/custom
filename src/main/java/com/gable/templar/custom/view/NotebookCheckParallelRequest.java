package com.gable.templar.custom.view;

import java.util.List;

public class NotebookCheckParallelRequest {
    List<String> noteRefIds;

    String runningId;

    Long moduleNotebookId;

    Integer concurrentParallel;

    String runBy;

    public void setNoteRefIds(List<String> noteRefIds) {
        this.noteRefIds = noteRefIds;
    }

    public List<String> getNoteRefIds() {
        return noteRefIds;
    }

    public void setModuleNotebookId(Long moduleNotebookId) {
        this.moduleNotebookId = moduleNotebookId;
    }

    public Long getModuleNotebookId() {
        return moduleNotebookId;
    }

    public void setRunningId(String runningId) {
        this.runningId = runningId;
    }

    public String getRunningId() {
        return runningId;
    }

    public void setConcurrentParallel(Integer concurrentParallel) {
        this.concurrentParallel = concurrentParallel;
    }

    public Integer getConcurrentParallel() {
        return concurrentParallel;
    }

    public void setRunBy(String runBy) {
        this.runBy = runBy;
    }

    public String getRunBy() {
        return runBy;
    }
}
