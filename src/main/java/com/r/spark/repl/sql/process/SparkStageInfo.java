package com.r.spark.repl.sql.process;

public class SparkStageInfo {
    private Integer stageId;
    private Integer currentAttemptId;
    private String submissionTime;
    private String name;
    private Integer totalTasks;
    private Integer numActiveTasks = 0;
    private Integer numCompleteTasks = 0;
    private Integer numFailedTasks = 0;

    public Integer getStageId() {
        return stageId;
    }

    public void setStageId(Integer stageId) {
        this.stageId = stageId;
    }

    public Integer getCurrentAttemptId() {
        return currentAttemptId;
    }

    public void setCurrentAttemptId(Integer currentAttemptId) {
        this.currentAttemptId = currentAttemptId;
    }

    public String getSubmissionTime() {
        return submissionTime;
    }

    public void setSubmissionTime(String submissionTime) {
        this.submissionTime = submissionTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getTotalTasks() {
        return totalTasks;
    }

    public void setTotalTasks(Integer totalTasks) {
        this.totalTasks = totalTasks;
    }

    public Integer getNumActiveTasks() {
        return numActiveTasks;
    }

    public void setNumActiveTasks(Integer numActiveTasks) {
        this.numActiveTasks = numActiveTasks;
    }

    public Integer getNumCompleteTasks() {
        return numCompleteTasks;
    }

    public void setNumCompleteTasks(Integer numCompleteTasks) {
        this.numCompleteTasks = numCompleteTasks;
    }

    public Integer getNumFailedTasks() {
        return numFailedTasks;
    }

    public void setNumFailedTasks(Integer numFailedTasks) {
        this.numFailedTasks = numFailedTasks;
    }
}
