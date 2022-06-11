package com.r.spark.repl.sql;

import com.r.spark.repl.sql.exception.InterpreterException;
import com.r.spark.repl.sql.exception.LivyException;
import com.r.spark.repl.sql.exception.SessionDeadException;
import com.r.spark.repl.sql.exception.SessionNotFoundException;
import com.r.spark.repl.sql.process.ConsoleProcessBar;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public class LivyInterpreter extends  LivyApp {
    protected static final Logger LOGGER = LoggerFactory.getLogger(LivyInterpreter.class);
    private static final String SESSION_NOT_FOUND_PATTERN = "\"Session '\\d+' not found.\"";
    protected volatile SessionInfo sessionInfo;
    private int sessionCreationTimeout = 60*3;
    private int maxLogLines = 1000;
    protected boolean displayAppInfo;
    private boolean restartDeadSession = true;
    protected LivyVersion livyVersion;
    private Map<String,Object> requestParms;
    public String statementTags;
    private Boolean isSlient;

    Set<Object> paragraphsToCancel = Collections.newSetFromMap(new ConcurrentHashMap<Object, Boolean>());
    private ConcurrentHashMap<String, Integer> paragraphId2StmtProgressMap = new ConcurrentHashMap<String,Integer>();

    public LivyInterpreter(Map<String,Object> requestParms) {
        this.restTemplate = createRestTemplate();
        this.requestParms = requestParms;
        livyURL = (String)requestParms.get("url");
        requestParms.remove("url");
        if(requestParms.containsKey("conf") &&
                ((Map<String, Object>)requestParms.get("conf")).containsKey("livy.rsc.session.creation.timeout")) {
            // 从requestParams中获取用户设置的超时时间
            sessionCreationTimeout = Integer.valueOf(((Map<String, Object>)requestParms.get("conf")).get("livy.rsc.session.creation.timeout").toString());
            ((Map<String, Object>)requestParms.get("conf")).remove("livy.rsc.session.creation.timeout");
        } else if(System.getenv().containsKey("SESSION_CREATION_TIMEOUT")){
            // 从环境变量中获取超时时间
            sessionCreationTimeout = Integer.valueOf(System.getenv("SESSION_CREATION_TIMEOUT"));
        }
    }

    public String getSessionKind(){
        return (String)this.requestParms.get("kind");
    }

    public String getUserName(){
        return (String)this.requestParms.get("proxyUser");
    }

    public void open() throws InterpreterException {
        try {
            initLivySession();
        } catch (LivyException e) {
            String msg = "Fail to create session, please check livy interpreter log and " +
                    "livy server log";
            throw new InterpreterException(msg, e);
        }
    }

    public void close() {
        if (sessionInfo != null) {
            closeSession(sessionInfo.id);
            sessionInfo = null;
        }
    }

    public void ping() {
        if(sessionInfo != null) {
            pingSession(sessionInfo.id);
        }
    }

    private void pingSession(int sessionId) {
        try {
            callRestAPI("/sessions/" + sessionId + "/connect", "POST");
        } catch (Exception e) {
            LOGGER.error(String.format("Error ping session for user with session ID: %s",
                    sessionId), e);
        }
    }

    protected void initLivySession() throws LivyException {
        this.sessionInfo = createSession(getUserName());
        if (displayAppInfo) {
            if (sessionInfo.appId == null) {
                sessionInfo.appId = extractAppId();
            }

            if (sessionInfo.appInfo == null ||
                    StringUtils.isEmpty(sessionInfo.appInfo.get("sparkUiUrl"))) {
                sessionInfo.webUIAddress = extractWebUIAddress();
            } else {
                sessionInfo.webUIAddress = sessionInfo.appInfo.get("sparkUiUrl");
            }
            LOGGER.info("Create livy session successfully with sessionId: {}, appId: {}, webUI: {}",
                    sessionInfo.id, sessionInfo.appId, sessionInfo.webUIAddress);
        } else {
            LOGGER.info("Create livy session successfully with sessionId: {}", this.sessionInfo.id);
        }
    }

    protected String extractAppId() throws LivyException{
        return sessionInfo.appId;
    }

    protected String extractWebUIAddress() throws LivyException{
        return sessionInfo.webUIAddress;
    }

    public SessionInfo getSessionInfo() {
        return sessionInfo;
    }

    public String getCodeType() {
        if (getSessionKind().equalsIgnoreCase("pyspark3")) {
            return "pyspark";
        }
        return getSessionKind();
    }

    // throws exception
    public String interpret(String st) throws LivyException {
        if (StringUtils.isEmpty(st)) {
            return "";
        }
        return interpret(st, null, this.displayAppInfo, false, false);
    }

    private SessionInfo createSession(String user)  throws LivyException {
        try {
            ProgressBar progressBar = new ProgressBar();
            ConsoleProcessBar bar = new ConsoleProcessBar(false);
            int progress = 0;
            SessionInfo sessionInfo = SessionInfo.fromJson(
                    callRestAPI("/sessions", "POST", gson.toJson(requestParms)));
            long start = System.currentTimeMillis();
            System.err.println("Session is creating");
            if(!sessionInfo.isReady()) {
                while (!sessionInfo.isReady()) {
                    if ((System.currentTimeMillis() - start) / 1000 > sessionCreationTimeout) {
                        String msg = "The creation of session " + sessionInfo.id + " is timeout within "
                                + sessionCreationTimeout + " seconds, appId: " + sessionInfo.appId
                                + ", log:\n" + StringUtils.join(getSessionLog(sessionInfo.id).log, "\n");
                        closeSession(sessionInfo.id);
                        throw new LivyException(msg);
                    }
                    Thread.sleep(pullStatusInterval);
                    sessionInfo = getSessionInfo(sessionInfo.id);
                    if(sessionInfo.state.equals("starting")){
                        progressBar.showBarByPoint(progress);
                        progress++;
                        if(progress >= 100) {
                            progress = 99;
                        }
                    }else if (sessionInfo.state.equals("idle")){
                        progressBar.showBarByPoint(100);
                        System.err.println("");
                        System.err.println(MessageFormat.format("Session {0} has been established.", sessionInfo.id));
                        System.err.println(MessageFormat.format("Application Id: {0}, Tracking URL: {1}", sessionInfo.appId, sessionInfo.appInfo.get("sparkUiUrl")));
                        String trackIngUrl = sessionInfo.appInfo.get("sparkUiUrl");
                        String proxyUrl = trackIngUrl+"api/v1/applications/"+sessionInfo.appId+"/stages?status=active";
                        bar.setProxyUrl(proxyUrl);
                        bar.setTrackingUrl(trackIngUrl);
                    }else{
                        System.err.println(MessageFormat.format("Session {0} is in state {1}, appId {2}", sessionInfo.id, sessionInfo.state,
                                sessionInfo.appId));
                    }
                    if (sessionInfo.isFinished()) {
                        bar.finishAll();
                        String msg = "Session " + sessionInfo.id + " is finished, appId: " + sessionInfo.appId
                                + ", log:\n" + StringUtils.join(getSessionLog(sessionInfo.id).log, "\n");
                        throw new LivyException(msg);
                    }
                    if(sessionInfo.isShutDown()||sessionInfo.isError()||sessionInfo.isDead()){
                        String msg = "Session " + sessionInfo.id + " start error, log:\n" + StringUtils.join(getSessionLog(sessionInfo.id).log, "\n");
                        throw new LivyException(msg);
                    }
                }
            } else {
                // 复用session
                progressBar.showBarByPoint(100);
                System.err.println("");
                System.err.println(MessageFormat.format("Session {0} has been reused.", sessionInfo.id));
                System.err.println(MessageFormat.format("Application Id: {0}, Tracking URL: {1}", sessionInfo.appId, sessionInfo.appInfo.get("sparkUiUrl")));
                String trackIngUrl = sessionInfo.appInfo.get("sparkUiUrl");
                String proxyUrl = trackIngUrl+"api/v1/applications/"+sessionInfo.appId+"/stages?status=active";
                bar.setProxyUrl(proxyUrl);
                bar.setTrackingUrl(trackIngUrl);
            }
            return sessionInfo;
        } catch (Exception e) {
            LOGGER.error("Error when creating livy session for user " + user, e);
            throw new LivyException(e);
        }
    }

    private SessionInfo getSessionInfo(int sessionId) throws LivyException {
        return SessionInfo.fromJson(callRestAPI("/sessions/" + sessionId, "GET"));
    }

    private SessionLog getSessionLog(int sessionId) throws LivyException {
        return SessionLog.fromJson(callRestAPI("/sessions/" + sessionId + "/log?size=" + maxLogLines,
                "GET"));
    }

    public String interpret(String code,
                                       String paragraphId,
                                       boolean displayAppInfo,
                                       boolean appendSessionExpired,
                                       boolean appendSessionDead) throws LivyException {
        return interpret(code, getSessionKind(),
                paragraphId, displayAppInfo, appendSessionExpired, appendSessionDead);
    }

    public String interpret(String code,
                                       String codeType,
                                       String paragraphId,
                                       boolean displayAppInfo,
                                       boolean appendSessionExpired,
                                       boolean appendSessionDead) throws LivyException {
        StatementInfo stmtInfo = null;
        boolean sessionExpired = false;
        boolean sessionDead = false;
        ProgressBar progressBar = new ProgressBar();
        try {
            try {
                //如果为空集成session tag
                if(this.statementTags == null) {
                    if (System.getenv("SPARK_YARN_TAGS") != null) {
                        String sessionTag = this.sessionInfo.appTag.split(",", -1)[0];
                        this.statementTags = sessionTag + "," + System.getenv("SPARK_YARN_TAGS");
                    } else {
                        this.statementTags = this.sessionInfo.appTag;
                    }
                }
                stmtInfo = executeStatement(new ExecuteRequest(code, codeType, this.statementTags));
            } catch (SessionNotFoundException e) {
                LOGGER.warn("Livy session {} is expired, new session will be created.", sessionInfo.id);
                sessionExpired = true;
                // we don't want to create multiple sessions because it is possible to have multiple thread
                // to call this method, like LivySparkSQLInterpreter which use ParallelScheduler. So we need
                // to check session status again in this sync block
                synchronized (this) {
                    if (isSessionExpired()) {
                        initLivySession();
                    }
                }
                stmtInfo = executeStatement(new ExecuteRequest(code, codeType, this.statementTags));
            } catch (SessionDeadException e) {
                sessionDead = true;
                if (restartDeadSession) {
                    LOGGER.warn("Livy session {} is dead, new session will be created.", sessionInfo.id);
                    close();
                    try {
                        open();
                    } catch (InterpreterException ie) {
                        throw new LivyException("Fail to restart livy session", ie);
                    }
                    stmtInfo = executeStatement(new ExecuteRequest(code, codeType, this.statementTags));
                } else {
                    throw new LivyException("%html <font color=\"red\">Livy session is dead somehow, " +
                            "please check log to see why it is dead, and then restart livy interpreter</font>");
                }
            }

            // pull the statement status
            while (!stmtInfo.isAvailable()) {
                if (paragraphId != null && paragraphsToCancel.contains(paragraphId)) {
                    cancel(stmtInfo.id, paragraphId);
                    return "Job is cancelled";
                }
                try {
                    Thread.sleep(pullStatusInterval);
                } catch (InterruptedException e) {
                    LOGGER.error("InterruptedException when pulling statement status.", e);
                    throw new LivyException(e);
                }
                stmtInfo = getStatementInfo(stmtInfo.id);
                if (stmtInfo == null) {
                    throw new LivyException("failed when execute code, stmtInfo is null");
                }
                // 如果执行时发生错误，stmtInfo的output中的status设置为error,检测error并抛出。
                if(stmtInfo.output != null && stmtInfo.output.status.equals("error")) {
                    StringBuilder sb = new StringBuilder();
                    for(String trace: stmtInfo.output.traceback) {
                        sb.append(trace).append("\n");
                    }
                    throw new LivyException("failed when execute code: " + stmtInfo.output.evalue + "\n" + sb);
                }
            }
            if (appendSessionExpired || appendSessionDead) {
                return appendSessionExpireDead(sessionExpired, sessionDead);
            } else {
                return getResultFromStatementInfo(stmtInfo, displayAppInfo);
            }
        } finally {
            if (paragraphId != null) {
                paragraphId2StmtProgressMap.remove(paragraphId);
                paragraphsToCancel.remove(paragraphId);
            }
        }
    }

    private void cancel(int id, String paragraphId) {
        if (livyVersion.isCancelSupported()) {
            try {
                LOGGER.info("Cancelling statement " + id);
                cancelStatement(id);
            } catch (LivyException e) {
                LOGGER.error("Fail to cancel statement " + id + " for paragraph " + paragraphId, e);
            } finally {
                paragraphsToCancel.remove(paragraphId);
            }
        } else {
            LOGGER.warn("cancel is not supported for this version of livy: " + livyVersion);
            paragraphsToCancel.clear();
        }
    }

    private boolean isSessionExpired() throws LivyException {
        try {
            getSessionInfo(sessionInfo.id);
            return false;
        } catch (SessionNotFoundException e) {
            return true;
        } catch (LivyException e) {
            throw e;
        }
    }

    private String appendSessionExpireDead(boolean sessionExpired,boolean sessionDead) {
        if (sessionExpired) {
            return "<font color=\"red\">Previous livy session is expired, new livy session is created. " +
                            "Paragraphs that depend on this paragraph need to be re-executed!</font>";
        }
        if (sessionDead) {
            return
                    "<font color=\"red\">Previous livy session is dead, new livy session is created. " +
                            "Paragraphs that depend on this paragraph need to be re-executed!</font>";
        }
        return "";
    }

    private String getResultFromStatementInfo(StatementInfo stmtInfo,
                                                         boolean displayAppInfo) {
        if (stmtInfo.output != null && stmtInfo.output.isError()) {
            StringBuilder sb = new StringBuilder();
            sb.append(stmtInfo.output.evalue);
            // in case evalue doesn't have newline char
            if (!stmtInfo.output.evalue.contains("\n")) {
                sb.append("\n");
            }
            if (stmtInfo.output.traceback != null) {
                sb.append(StringUtils.join(stmtInfo.output.traceback));
            }
            return sb.toString();
        } else if (stmtInfo.isCancelled()) {
            // corner case, output might be null if it is cancelled.
            return "Job is cancelled";
        } else if (stmtInfo.output == null) {
            // This case should never happen, just in case
            return "Empty output";
        } else {
            //TODO(zjffdu) support other types of data (like json, image and etc)
            String result = stmtInfo.output.data.plainText;

            // check table magic result first
            if (stmtInfo.output.data.applicationLivyTableJson != null) {
                StringBuilder outputBuilder = new StringBuilder();
                boolean notFirstColumn = false;

                for (Map header : stmtInfo.output.data.applicationLivyTableJson.headers) {
                    if (notFirstColumn) {
                        outputBuilder.append("\t");
                    }
                    outputBuilder.append(header.get("name"));
                    notFirstColumn = true;
                }

                outputBuilder.append("\n");
                for (List<Object> row : stmtInfo.output.data.applicationLivyTableJson.records) {
                    outputBuilder.append(StringUtils.join(row, "\t"));
                    outputBuilder.append("\n");
                }
                return outputBuilder.toString();
            } else if (result != null) {
                result = result.trim();
                if (result.startsWith("<link")
                        || result.startsWith("<script")
                        || result.startsWith("<style")
                        || result.startsWith("<div")) {
                    result = "%html " + result;
                }
            }

            if (displayAppInfo) {
                String appInfoHtml = "<hr/>Spark Application Id: " + sessionInfo.appId + "<br/>"
                        + "Spark WebUI: <a href=\"" + sessionInfo.webUIAddress + "\">"
                        + sessionInfo.webUIAddress + "</a>";
                return appInfoHtml+result;
            } else {
                return result;
            }
        }
    }

    private StatementInfo executeStatement(ExecuteRequest executeRequest)
            throws LivyException {
        return StatementInfo.fromJson(callRestAPI("/sessions/" + sessionInfo.id + "/statements", "POST",
                executeRequest.toJson()));
    }

    private StatementInfo getStatementInfo(int statementId)
            throws LivyException {
        return StatementInfo.fromJson(
                callRestAPI("/sessions/" + sessionInfo.id + "/statements/" + statementId, "GET"));
    }

    private void cancelStatement(int statementId) throws LivyException {
        callRestAPI("/sessions/" + sessionInfo.id + "/statements/" + statementId + "/cancel", "POST");
    }

    private String callRestAPI(String targetURL, String method) throws LivyException {
        return callRestAPI(targetURL, method, "");
    }

    private void closeSession(int sessionId) {
        try {
            callRestAPI("/sessions/" + sessionId, "DELETE");
        } catch (Exception e) {
            LOGGER.error(String.format("Error closing session for user with session ID: %s",
                    sessionId), e);
        }
    }

    /**
     *
     */
    public static class SessionInfo {

        public final int id;
        public String appId;
        public String appTag;
        public String webUIAddress;
        public final String owner;
        public final String proxyUser;
        public final String state;
        public final String kind;
        public final Map<String, String> appInfo;
        public final List<String> log;

        public SessionInfo(int id, String appId, String appTag, String owner, String proxyUser, String state,
                           String kind, Map<String, String> appInfo, List<String> log) {
            this.id = id;
            this.appId = appId;
            this.appTag = appTag;
            this.owner = owner;
            this.proxyUser = proxyUser;
            this.state = state;
            this.kind = kind;
            this.appInfo = appInfo;
            this.log = log;
        }

        public boolean isReady() {
            return state.equals("idle");
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

        public boolean isFinished() {
            return state.equals("error") || state.equals("dead") || state.equals("success");
        }

        public static SessionInfo fromJson(String json) {
            return gson.fromJson(json, SessionInfo.class);
        }
    }

    private static class SessionLog {
        public int id;
        public int from;
        public int size;
        public List<String> log;

        SessionLog() {
        }

        public static SessionLog fromJson(String json) {
            return gson.fromJson(json, SessionLog.class);
        }
    }

    static class ExecuteRequest {
        public final String code;
        public final String kind;
        public final String tag;

        ExecuteRequest(String code, String kind,String tag) {
            this.code = code;
            this.kind = kind;
            this.tag = tag;
        }

        public String toJson() {
            return gson.toJson(this);
        }
    }

    private static class StatementInfo {
        public Integer id;
        public String state;
        public String tag;
        public double progress;
        public StatementOutput output;
        public Map<Integer,Double> jobProgress;

        StatementInfo() {
        }

        public static StatementInfo fromJson(String json) {
            String rightJson = "";
            try {
                gson.fromJson(json, StatementInfo.class);
                rightJson = json;
            } catch (Exception e) {
                e.printStackTrace();
                if (json.contains("\"traceback\":{}")) {
                    LOGGER.debug("traceback type mismatch, replacing the mismatching part ");
                    rightJson = json.replace("\"traceback\":{}", "\"traceback\":[]");
                    LOGGER.debug("new json string is {}", rightJson);
                }
            }
            return gson.fromJson(rightJson, StatementInfo.class);
        }

        public boolean isAvailable() {
            return state.equals("available") || state.equals("cancelled");
        }
        public boolean isRunning() {
            return state.equals("running");
        }

        public boolean isCancelled() {
            return state.equals("cancelled");
        }

        private static class StatementOutput {
            public String status;
            public String executionCount;
            public Data data;
            public String ename;
            public String evalue;
            public String[] traceback;
            public TableMagic tableMagic;

            public boolean isError() {
                return status.equals("error");
            }

            public String toJson() {
                return gson.toJson(this);
            }

            private static class Data {
                @SerializedName("text/plain")
                public String plainText;
                @SerializedName("image/png")
                public String imagePng;
                @SerializedName("application/json")
                public String applicationJson;
                @SerializedName("application/vnd.livy.table.v1+json")
                public TableMagic applicationLivyTableJson;
            }

            private static class TableMagic {
                @SerializedName("headers")
                List<Map> headers;

                @SerializedName("data")
                List<List> records;
            }
        }
    }

    public Map<String, Object> getRequestParms() {
        return requestParms;
    }

    public Boolean getSlient() {
        return isSlient;
    }

    public void setSlient(Boolean slient) {
        isSlient = slient;
    }
}