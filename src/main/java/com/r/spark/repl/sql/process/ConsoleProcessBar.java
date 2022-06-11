package com.r.spark.repl.sql.process;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;


public class ConsoleProcessBar {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsoleProcessBar.class);
    public static Integer TerminalWidth = 0;
    private Long lastFinishTime = 0L;
    private Long lastUpdateTime = 0L;
    private String lastProgressBar = "";
    private Long firstDelayMSec = 500L;
    private Integer updatePeriodMSec = 200;
    private String proxyUrl = "http://rm/proxy/application_1575020200992_11271829/api/v1/applications/application_1575020200992_11271829/stages?status=active";
    private String trackingUrl;
    private char CR = '\r';
    private Timer timer = new Timer("refresh progress",true);
    private RestTemplate restTemplate = new RestTemplate();
    protected static Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();
    private static final SimpleDateFormat SDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'GMT'");
    static {
        if(System.getenv().get("COLUMNS")!=null){
            TerminalWidth = Integer.valueOf(System.getenv().get("COLUMNS"));
        } else {
            TerminalWidth = 80;
        }
        SDF.setTimeZone(TimeZone.getTimeZone("GMT"));
    }
    public ConsoleProcessBar(boolean isSlient){
        if(!isSlient) {
            System.out.println("XXXXXXXX");
            this.timer = new Timer("refresh progress", true);
            System.out.println("XXXXXXXX");
            //get active StageIds
            List<SparkStageInfo> stages = new ArrayList<SparkStageInfo>();
            System.out.println(proxyUrl);
            if (!org.springframework.util.StringUtils.isEmpty(proxyUrl)) {
                // restTemplate.getForObject()
                HttpHeaders headers = new HttpHeaders();
                headers.add("Content-Type", MediaType.APPLICATION_JSON_UTF8_VALUE);
                HttpEntity<String> entity = new HttpEntity<String>(headers);
                ResponseEntity<String> response = restTemplate.exchange(proxyUrl, HttpMethod.GET, entity, String.class);
                System.out.println(response.getStatusCode().value() );
                if (response.getStatusCode().value() == 200
                        || response.getStatusCode().value() == 201) {
                    String json = response.getBody();
                    System.out.println("XXXXXXXX json"  + json);
                    stages = gson.fromJson(json, new TypeToken<List<SparkStageInfo>>() {
                    }.getType());
                }
            }
            List<SparkStageInfo> showStages = new ArrayList<SparkStageInfo>();
            for (SparkStageInfo sparkStageInfo : stages) {
                try {
                    Integer totalTasks = getTotalTasks(sparkStageInfo);
                        showStages.add(sparkStageInfo);
                } catch (Throwable e) {
                    LOGGER.error(e.getMessage());
                }
            }
            System.out.println(showStages.size());
            if (showStages.size() > 0) {
                show(11, stages.subList(0, Math.min(stages.size(), 3)));
            }
//            timer.schedule(new TimerTask() {
//                @Override
//                public void run() {
//                    System.out.println("XXXXXXXX");
//                    try {
//                        long now = System.currentTimeMillis();
////                        if (now - lastFinishTime < firstDelayMSec) {
////                            return;
////                        }
//                        //get active StageIds
//                        List<SparkStageInfo> stages = new ArrayList<SparkStageInfo>();
//                        if (!org.springframework.util.StringUtils.isEmpty(proxyUrl)) {
//                            // restTemplate.getForObject()
//                            HttpHeaders headers = new HttpHeaders();
//                            headers.add("Content-Type", MediaType.APPLICATION_JSON_UTF8_VALUE);
//                            HttpEntity<String> entity = new HttpEntity<String>(headers);
//                            ResponseEntity<String> response = restTemplate.exchange(proxyUrl, HttpMethod.GET, entity, String.class);
//                            if (response.getStatusCode().value() == 200
//                                    || response.getStatusCode().value() == 201) {
//                                String json = response.getBody();
//                                stages = gson.fromJson(json, new TypeToken<List<SparkStageInfo>>() {
//                                }.getType());
//                            }
//                        }
//                        List<SparkStageInfo> showStages = new ArrayList<SparkStageInfo>();
//                        for (SparkStageInfo sparkStageInfo : stages) {
//                            try {
//                                Integer totalTasks = getTotalTasks(sparkStageInfo);
//                                if (totalTasks > 0 && now > (SDF.parse(sparkStageInfo.getSubmissionTime()).getTime() + firstDelayMSec)) {
//                                    showStages.add(sparkStageInfo);
//                                }
//                            } catch (Throwable e) {
//                                LOGGER.error(e.getMessage());
//                            }
//                        }
//                        System.out.println(showStages.size());
//                        if (showStages.size() > 0) {
//                            show(now, stages.subList(0, Math.min(stages.size(), 3)));
//                        }
//                    } catch (Throwable e) {
//                        try {
//                            Thread.sleep(2 * 1000l);
//                        } catch (InterruptedException ex) {
//                            LOGGER.error(ex.getMessage());
//                        }
//                    }
//                }
//            }, firstDelayMSec, updatePeriodMSec);
        }
    }

    private Integer getTotalTasks(SparkStageInfo sparkStageInfo) {
        return sparkStageInfo.getTotalTasks();
    }


    public void show(long now, List<SparkStageInfo> stages){
        System.out.println("show  ..   .. ");
        Integer width = TerminalWidth / stages.size();
        StringBuffer processBarOut = new StringBuffer();
        for(SparkStageInfo s:stages){
            int total = this.getTotalTasks(s);
            if(total==0){
                continue;
            }
            String header = "[Stage "+s.getStageId()+":";
            String tailer = "("+s.getNumCompleteTasks()+" + "+s.getNumActiveTasks()+") / "+total+"]";
            Integer w = width - header.length() - tailer.length();
            String bar = "";
            if(w>0){
                Integer percent = w * s.getNumCompleteTasks() / total;
                StringBuffer processBar = new StringBuffer();
                for(int i=0;i<w;i++){
                    if(i<percent){
                        processBar.append("=");
                    } else if (i==percent){
                        processBar.append(">");
                    }else{
                        processBar.append(" ");
                    }
                }
                bar = processBar.toString();
            }else{
                bar = "";
            }
            processBarOut.append(header+bar+tailer);
        }
        String barStr = processBarOut.toString();
        if(!barStr.equals(lastProgressBar)|| now - lastUpdateTime > 10 * 1000L){
            System.err.println(CR + barStr);
            lastUpdateTime = now;
        }
        lastProgressBar = barStr;
    }
    private void clear() {
        if (!lastProgressBar.isEmpty()) {
            StringBuffer space = new StringBuffer();
            for (int i = 0; i < TerminalWidth; i++) {
                space.append(" ");
            }
            System.err.println(CR + space.toString() + CR);
            lastProgressBar = "";
        }
    }
    public void finishAll(){
        clear();
        lastFinishTime = System.currentTimeMillis();
    }

    public String getProxyUrl() {
        return proxyUrl;
    }

    public void setProxyUrl(String proxyUrl) {
        this.proxyUrl = proxyUrl;
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public void setTrackingUrl(String trackingUrl) {
        this.trackingUrl = trackingUrl;
    }


    public static void main(String[] args) {
        ConsoleProcessBar bar = new ConsoleProcessBar(false);
        String trackIngUrl = "http://rm/proxy/application_1575020200992_11271829";
        String proxyUrl = "http://rm/proxy/application_1575020200992_11271829/api/v1/applications/application_1575020200992_11271829/stages?status=active";
        System.out.println(proxyUrl);
        bar.setProxyUrl(proxyUrl);
        bar.setTrackingUrl(trackIngUrl);
        System.out.println(MessageFormat.format("Application Id: {0}, Tracking URL: {1}",
                "application_1575020200992_11271829", bar.getTrackingUrl() ));
    }
}
