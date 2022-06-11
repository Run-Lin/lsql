package com.r.spark.repl.sql;

import com.r.spark.repl.sql.exception.LivyException;
import com.r.spark.repl.sql.process.ConsoleProcessBar;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

public class LivyBatch extends LivyApp{
    private static final Logger LOGGER = LoggerFactory.getLogger(LivyBatch.class);
    Map<String,Object> requestParms = null;
    String[] args = null;
    private static Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
    ConsoleProcessBar bar = new ConsoleProcessBar(false);
    public LivyBatch(Map<String,Object> requestParms,String[] args){
        this.requestParms = requestParms;
        if(args!=null && args.length > 0 ){
            this.requestParms.put("args",args);
        }
        livyURL = (String)requestParms.get("url");
        requestParms.remove("url");
        requestParms.remove("kind");
        this.restTemplate = createRestTemplate();
    }
    public int executor(){
        LOGGER.debug("[LivyBatch.executor][begin]");
        try {
            BatchInfo batchInfo = BatchInfo.fromJson(callRestAPI("/batches", "POST", gson.toJson(requestParms)));
            boolean hasAppId = false , hasAppInfo = false;
            while(!batchInfo.isFinished()){
                Thread.sleep(pullStatusInterval);
                if(!StringUtils.isEmpty(batchInfo.appId) && !hasAppId){
                    System.err.println("Application Id: "+batchInfo.appId);
                    Runtime.getRuntime().addShutdownHook(new Thread(new CleanBatch(batchInfo.id)));
                    hasAppId = true;
                }
                if(!StringUtils.isEmpty(batchInfo.appInfo.get("driverLogUrl")) && !hasAppInfo){
                    String trackIngUrl = batchInfo.appInfo.get("sparkUiUrl");
                    String proxyUrl = trackIngUrl+"api/v1/applications/"+batchInfo.appId+"/stages?status=active";
                    bar.setProxyUrl(proxyUrl);
                    bar.setTrackingUrl(trackIngUrl);
                    System.err.println(MessageFormat.format("Application Id: {0}, Tracking URL: {1}",
                            batchInfo.appId, bar.getTrackingUrl() ));
                    System.err.println("DriverLogUrl: "+ batchInfo.appInfo.get("driverLogUrl"));
                    hasAppInfo = true;
                }
                batchInfo = getBatchInfo(batchInfo.id);
                if(batchInfo.isFinished()||batchInfo.isShutDown()){
                    bar.finishAll();
                    System.err.println("The Application Id: "+batchInfo.appId+" finished State: "+batchInfo.state);
                    return batchInfo.isSuccess() ? 0 : 1;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
        LOGGER.debug("[LivyBatch.executor][end]");
        return 1;
    }
    private BatchInfo getBatchInfo(int bid) throws LivyException {
        return BatchInfo.fromJson(callRestAPI("/batches/" + bid, "GET",""));
    }
    public class CleanBatch implements Runnable {
        private int bid = 0;
        private CleanBatch(int bid){
            this.bid = bid;
        }
        @Override
        public void run() {
            try {
                if(bid > 0){
                    callRestAPI("/batches/"+bid, "DELETE","");
                }
            } catch (LivyException e) {
                System.err.println("can not delete batch id:"+bid+" msg:"+e.getMessage());
            }
        }
    }

    public static class BatchInfo {

        public final int id;
        public String appId;
        public String webUIAddress;
        public final String owner;
        public final String proxyUser;
        public final String state;
        public final Map<String, String> appInfo;
        public final List<String> log;

        public BatchInfo(int id, String appId, String owner, String proxyUser, String state,
                           Map<String, String> appInfo, List<String> log) {
            this.id = id;
            this.appId = appId;
            this.owner = owner;
            this.proxyUser = proxyUser;
            this.state = state;
            this.appInfo = appInfo;
            this.log = log;
        }

        public boolean isStaring() {
            return state.equals("starting");
        }
        public boolean isRunning(){
            return state.equals("running");
        }
        public boolean isShutDown(){
            return state.equals("shutting_down");
        }
        public boolean isError(){
            return state.equals("error");
        }
        public boolean isDead(){
            return state.equals("dead");
        }
        public boolean isSuccess(){ return state.equals("success");}

        public boolean isFinished() {
            return state.equals("error") || state.equals("dead") || state.equals("success");
        }

        public static BatchInfo fromJson(String json) {
            return gson.fromJson(json, BatchInfo.class);
        }
    }


}
