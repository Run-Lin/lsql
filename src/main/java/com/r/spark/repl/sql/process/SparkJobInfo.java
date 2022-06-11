package com.r.spark.repl.sql.process;

import java.io.Serializable;
import java.util.List;


public class SparkJobInfo implements Serializable{
    private Integer jobId;
    private String name;
    private String submissionTime;
    private List<Integer> stageIds;
    private String status;
    private Integer numTasks;
    private Integer numActiveTasks;
    private Integer numCompletedTasks;
    private Integer numSkippedTasks;
    private Integer numFailedTasks;
    private Integer numActiveStages;
    private Integer numCompletedStages;
    private Integer numSkippedStages;
    private Integer numFailedStages;

    public Integer getJobId() {
        return jobId;
    }

    public void setJobId(Integer jobId) {
        this.jobId = jobId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSubmissionTime() {
        return submissionTime;
    }

    public void setSubmissionTime(String submissionTime) {
        this.submissionTime = submissionTime;
    }

    public List<Integer> getStageIds() {
        return stageIds;
    }

    public void setStageIds(List<Integer> stageIds) {
        this.stageIds = stageIds;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Integer getNumTasks() {
        return numTasks;
    }

    public void setNumTasks(Integer numTasks) {
        this.numTasks = numTasks;
    }

    public Integer getNumActiveTasks() {
        return numActiveTasks;
    }

    public void setNumActiveTasks(Integer numActiveTasks) {
        this.numActiveTasks = numActiveTasks;
    }

    public Integer getNumCompletedTasks() {
        return numCompletedTasks;
    }

    public void setNumCompletedTasks(Integer numCompletedTasks) {
        this.numCompletedTasks = numCompletedTasks;
    }

    public Integer getNumSkippedTasks() {
        return numSkippedTasks;
    }

    public void setNumSkippedTasks(Integer numSkippedTasks) {
        this.numSkippedTasks = numSkippedTasks;
    }

    public Integer getNumFailedTasks() {
        return numFailedTasks;
    }

    public void setNumFailedTasks(Integer numFailedTasks) {
        this.numFailedTasks = numFailedTasks;
    }

    public Integer getNumActiveStages() {
        return numActiveStages;
    }

    public void setNumActiveStages(Integer numActiveStages) {
        this.numActiveStages = numActiveStages;
    }

    public Integer getNumCompletedStages() {
        return numCompletedStages;
    }

    public void setNumCompletedStages(Integer numCompletedStages) {
        this.numCompletedStages = numCompletedStages;
    }

    public Integer getNumSkippedStages() {
        return numSkippedStages;
    }

    public void setNumSkippedStages(Integer numSkippedStages) {
        this.numSkippedStages = numSkippedStages;
    }

    public Integer getNumFailedStages() {
        return numFailedStages;
    }

    public void setNumFailedStages(Integer numFailedStages) {
        this.numFailedStages = numFailedStages;
    }

}
