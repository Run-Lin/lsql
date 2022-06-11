package com.r.spark.repl.sql;

import com.r.spark.repl.sql.common.Utils;
import jline.console.ConsoleReader;
import jline.console.history.FileHistory;
import jline.console.history.History;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class LivyCli {

    private static Logger LOG = LoggerFactory.getLogger(LivyCli.class);
    protected ConsoleReader reader;
    protected LivyInterpreter interpreter = null;
    protected List<String> hiveArgs;
    public LivyCli(Map<String,Object> requestParms, List<String> args){
        uploadFiles(requestParms);
        interpreter = new LivyInterpreter(requestParms);
        hiveArgs = args;
        interpreter.setSlient(this.isSilent());
    }

    public void checkAndSetHiveArgs() {
        // check hive args
        if (hiveArgs != null && hiveArgs.size() == 0) {
            return;
        }
        OptionsProcessor optionsProcessor = new OptionsProcessor();
        String[] hiveArgsArray = new String[hiveArgs.size()];
        hiveArgs.toArray(hiveArgsArray);
        optionsProcessor.process_stage1(hiveArgsArray);
        String hiveArgsStr = optionsProcessor.process_stage2(hiveArgsArray);
        int res = processLine(hiveArgsStr);
        if(res != 0) {
            LOG.error("input wrong hive args:" + hiveArgsStr + ", please recheck!");
        }
        try {
            processInitFiles(optionsProcessor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 暴露接口
    public boolean isSilent() {
        return hiveArgs.contains("-e") || hiveArgs.contains("-f");
    }

    public String getExecString() {
        int index = hiveArgs.indexOf("-e");
        if (index == -1) {
            return null;
        } else {
            return hiveArgs.get(index + 1);
        }
    }

    public String getFilePath() {
        int index = hiveArgs.indexOf("-f");
        if (index == -1) {
            return null;
        } else {
            return hiveArgs.get(index + 1);
        }
    }

    private static String spacesForString(String s) {
        return s != null && s.length() != 0?String.format("%1$-" + s.length() + "s", new Object[]{""}):"";
    }

    protected void setupConsoleReader() throws IOException{
        this.reader = new ConsoleReader();
        this.reader.setExpandEvents(false);
        this.reader.setBellEnabled(false);
        this.setupCmdHistory();
    }
    private void setupCmdHistory(){
        String historyDirectory = System.getProperty("user.home");
        FileHistory history = null;

        try {
            if((new File(historyDirectory)).exists()) {
                String e = historyDirectory + File.separator + ".hivehistory";
                history = new FileHistory(new File(e));
                this.reader.setHistory(history);
            } else {
                System.err.println("WARNING: Directory for Hive history file: " + historyDirectory + " does not exist.   History will not be available during this session.");
            }
        } catch (Exception var5) {
            System.err.println("WARNING: Encountered an error while trying to initialize Hive\'s history file.  History will not be available during this session.");
            System.err.println(var5.getMessage());
        }
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                interpreter.close();
                History h = reader.getHistory();
                if(h instanceof FileHistory) {
                    try {
                        ((FileHistory)h).flush();
                    } catch (Throwable var3) {
                        System.err.println("WARNING: Failed to write command history file: " + var3.getMessage());
                    }
                }
            }
        }));

    }

    public int executor() throws Exception{
        this.interpreter.open();
        try {
            this.setupConsoleReader();
            int ret = 0;
            String prefix = "";
            String curDB = "";
            String prompt2 = spacesForString(this.interpreter.getCodeType());
            String curPrompt = replaceSqlKind(this.interpreter.getCodeType()) + curDB;
            String dbSpaces = spacesForString(curDB);
            boolean isSilent = this.isSilent();
            // 添加hiveconf参数
            this.checkAndSetHiveArgs();
            // 判断-e / -f
            if(isSilent) {
                if(this.getExecString() != null) {
                    String execString = this.getExecString();
                    LOG.info(execString);
                    int res = processLine(execString, true);
                    return res;
                }
                if(this.getFilePath() != null){
                    String filePath = this.getFilePath();
                    int res = processFile(filePath);
                    return res;
                }
                LOG.error("wrong parameters in -e or -f, please check.");
                return 1;
            } else {
                while(true) {
                    String line;
                    while((line = this.reader.readLine(curPrompt + "> ")) != null) {
                        if(!prefix.equals("")) {
                            prefix = prefix + '\n';
                        }
                        if(line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
                            line = prefix + line;
                            ret = processLine(line.substring(0,line.length()-1), true);
                            prefix = "";
                            curDB = "";
                            curPrompt = replaceSqlKind(this.interpreter.getCodeType()) + curDB;
                            dbSpaces = dbSpaces.length() == curDB.length()?dbSpaces:spacesForString(curDB);
                        } else {
                            prefix = prefix + line;
                            curPrompt = prompt2 + dbSpaces;
                        }
                    }
                    return ret;
                }
            }
        } finally {
            this.interpreter.close();
        }
    }

    public static void main(String[] args) {
        try {
            MainOptionParser parser = new MainOptionParser();
            Map<String,Object> argMap = parser.parseArgs(args);
            if(argMap.containsKey("silent")){
                LivyBatch batch = new LivyBatch(argMap,null);
                batch.executor();
            } else {
                List<String> hiveArgs = new ArrayList<>();
                LivyCli cli = new LivyCli(argMap, hiveArgs);
                cli.executor();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public int processFile(String fileName) throws IOException {
        Path path = new Path(fileName);
        FileSystem fs;
        Configuration conf = new Configuration();
        Properties properties = Utils.loadHDFSProperties();
        if(properties == null) {
            System.err.println("no properties!");
            System.exit(1);
        }
        conf.addResource(new URL(properties.get("hdfs-site.xml").toString()));
        conf.addResource(new URL(properties.get("core-site.xml").toString()));
        Object mountTable = properties.get("mount-table.xml");
        if(mountTable != null && mountTable.toString().trim().length() > 0) {
            conf.addResource(new URL(properties.get("mount-table.xml").toString()));
        }
        conf.set("fs.defaultFS", properties.get("fs.defaultFS").toString());
        conf.set("fs.hdfs.impl", properties.get("fs.hdfs.impl").toString());

        if (!path.toUri().isAbsolute()) {
            fs = FileSystem.getLocal(conf);
            path = fs.makeQualified(path);
        } else {
            fs = FileSystem.get(path.toUri(), conf);
        }
        long start = System.currentTimeMillis();
        BufferedReader bufferReader = null;
        int rc = 0;
        try {
            bufferReader = new BufferedReader(new InputStreamReader(fs.open(path)));
            rc = processReader(bufferReader);
        } finally {
            IOUtils.closeStream(bufferReader);
        }
        long end = System.currentTimeMillis();
        double timeTaken = (end - start) / 1000.0;
        System.err.println("Time taken: " + timeTaken + " seconds");
        return rc;
    }

    public int processReader(BufferedReader r) throws IOException {
        String line;
        StringBuilder qsb = new StringBuilder();

        while ((line = r.readLine()) != null) {
            // Skipping through comments
            if (! line.trim().startsWith("--")) {
                qsb.append(line + "\n");
            }
        }
        return (processLine(qsb.toString()));
    }

    public int processLine(String line) {
        return processLine(line, false);
    }

    public int processLine(String line, boolean allowInterrupting) {
        SignalHandler oldSignal = null;
        Signal interruptSignal = null;

        if (allowInterrupting) {
            // Remember all threads that were running at the time we started line processing.
            // Hook up the custom Ctrl+C handler while processing this line
            interruptSignal = new Signal("INT");
            oldSignal = Signal.handle(interruptSignal, new SignalHandler() {
                private final Thread cliThread = Thread.currentThread();
                private boolean interruptRequested;
                public void handle(Signal signal) {
                    boolean initialRequest = !interruptRequested;
                    interruptRequested = true;
                    // Kill the VM on second ctrl+c
                    if (!initialRequest) {
                        System.exit(127);
                    }
                }
            });
        }

        try {
            int lastRet = 0, ret = 0;

            String command = "";
            for (String oneCmd : line.split(";")) {

                if (StringUtils.endsWith(oneCmd, "\\")) {
                    command += StringUtils.chop(oneCmd) + ";";
                    continue;
                } else {
                    command += oneCmd;
                }
                if (StringUtils.isBlank(command)) {
                    continue;
                }
                lastRet = processCmd(command);
                if(isSilent() && lastRet != 0) {
                    System.exit(lastRet);
                }
                command = "";
            }
            return lastRet;
        } finally {
            // Once we are done processing the line, restore the old handler
            if (oldSignal != null && interruptSignal != null) {
                Signal.handle(interruptSignal, oldSignal);
            }
        }
    }

    public int processCmd(String cmd) {

        String cmd_trimmed = cmd.trim();
        int ret = 0;
        if (cmd_trimmed.toLowerCase().equals("quit") || cmd_trimmed.toLowerCase().equals("exit")) {
            System.exit(0);
        } else if (cmd_trimmed.toLowerCase().contains("livy.statement.tags")) {
            String firstToken = cmd_trimmed.split("\\s+")[0].trim();
            if (firstToken.equalsIgnoreCase("set")) {
                String[] part = new String[2];
                String nwcmd = cmd.substring(firstToken.length()).trim();
                int eqIndex = nwcmd.indexOf('=');
                if (nwcmd.contains("=")){
                    if (eqIndex == nwcmd.length() - 1) { //x=
                        part[0] = nwcmd.substring(0, nwcmd.length() - 1);
                        part[1] = "";
                    } else { //x=y
                        part[0] = nwcmd.substring(0, eqIndex).trim();
                        part[1] = nwcmd.substring(eqIndex + 1).trim();
                    }
                    if(!StringUtils.isEmpty(part[1])){
                        this.interpreter.statementTags = part[1];
                    } else {
                        this.interpreter.statementTags = this.interpreter.sessionInfo.appTag;
                    }
                }
            }
            ret = 0;
        } else {
            try {
                ret = processLocalCmd(cmd);
            } catch (Throwable e) {
                // console.printError("Failed processing command " + tokens[0] + " " + e.getLocalizedMessage(),
                //        org.apache.hadoop.util.StringUtils.stringifyException(e));
                ret = 1;
            }
        }

        return ret;
    }

    private String[] tokenizeCmd(String cmd) {
        return cmd.split("\\s+");
    }

    /**
     * Extract and clean up the first command in the input.
     */
    private String getFirstCmd(String cmd, int length) {
        return cmd.substring(length).trim();
    }

    int processLocalCmd(String cmd) {
        int tryCount = 1;
        boolean needRetry;
        int ret = 0;
        do {
            try {
                needRetry = false;
                PrintStream out = new PrintStream(System.out, true, "UTF-8");
                out.println(interpreter.interpret(cmd));
                // 运行成功,ret赋值
                ret = 0;
            } catch (Throwable e) {
                e.printStackTrace();
                System.err.println("Retry query with a different approach... ");
                tryCount--;
                needRetry = true;
                ret = 1;
            }
        } while (needRetry && tryCount > 0);
        return ret;
    }
    private static String replaceSqlKind(String kind){
        if(kind.equalsIgnoreCase("sql2")){
            return "sql";
        }
        return kind;
    }

    public void processInitFiles(OptionsProcessor processor) throws IOException {
        boolean saveSilent = processor.getIsSilent();
        processor.setIsSilent(true);
        for (String initFile : processor.getInitFiles()) {
            int rc = processFile(initFile);
            if (rc != 0) {
                System.exit(rc);
            }
        }
        if (processor.getInitFiles().size() == 0) {
            if (System.getenv("HIVE_HOME") != null) {
                String hivercDefault = System.getenv("HIVE_HOME") + File.separator +
                        "bin" + File.separator + processor.HIVERCFILE;
                if (new File(hivercDefault).exists()) {
                    int rc = processFile(hivercDefault);
                    if (rc != 0) {
                        System.exit(rc);
                    }
                    LOG.error("Putting the global hiverc in " +
                            "$HIVE_HOME/bin/.hiverc is deprecated. Please "+
                            "use $HIVE_CONF_DIR/.hiverc instead.");
                }
            }
            if (System.getenv("HIVE_CONF_DIR") != null) {
                String hivercDefault = System.getenv("HIVE_CONF_DIR") + File.separator
                        + processor.HIVERCFILE;
                if (new File(hivercDefault).exists()) {
                    int rc = processFile(hivercDefault);
                    if (rc != 0) {
                        System.exit(rc);
                    }
                }
            }
            if (System.getProperty("user.home") != null) {
                String hivercUser = System.getProperty("user.home") + File.separator +
                        processor.HIVERCFILE;
                if (new File(hivercUser).exists()) {
                    int rc = processFile(hivercUser);
                    if (rc != 0) {
                        System.exit(rc);
                    }
                }
            }
        }
        processor.setIsSilent(saveSilent);
    }

    public void uploadFiles(Map<String, Object> requestParams) {
        Map<String, Object> confMap;
        if(requestParams.get("conf") instanceof Map) {
            confMap = (Map<String, Object>)requestParams.get("conf");
        } else {
            return;
        }
        Properties properties = Utils.loadHDFSProperties();
        if(properties == null) {
            System.err.println("no properties!");
            System.exit(1);
        }
        if (confMap.get("spark.yarn.dist.pyFiles") != null
                && ((String)confMap.get("spark.yarn.dist.pyFiles")).length() > 0) {
            String[] pyFiles = ((String)confMap.get("spark.yarn.dist.pyFiles")).split(",");
            List<String> hdfsPyFiles = new ArrayList<>();
            try {
                for(int i = 0; i < pyFiles.length; i++) {
                    pyFiles[i] = (LivyApp.uploadLocalFile(pyFiles[i]));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            requestParams.put("pyFiles", hdfsPyFiles);
            requestParams.put("pyFiles", pyFiles);
            confMap.remove("spark.yarn.dist.pyFiles");
        }

        if (confMap.get("spark.yarn.dist.files")!= null
                && ((String)confMap.get("spark.yarn.dist.files")).length() > 0) {
            String[] files = ((String)confMap.get("spark.yarn.dist.files")).split(",");
            StringBuilder hdfsFilesStr = new StringBuilder();
            try {
                for(String str: files) {
                    hdfsFilesStr.append(LivyApp.uploadLocalFile(str)).append(",");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            hdfsFilesStr.deleteCharAt(hdfsFilesStr.length() - 1);
            confMap.replace("spark.yarn.dist.files", hdfsFilesStr.toString());
        }
        if(confMap.containsKey("spark.files")) {
            confMap.remove("spark.files");
        }

        if (confMap.get("spark.yarn.dist.jars") != null
                && ((String)confMap.get("spark.yarn.dist.jars")).length() > 0) {
            String[] jarFiles = ((String)confMap.get("spark.yarn.dist.jars")).split(",");
            StringBuilder jarFilesStr = new StringBuilder();
            try {
               for(String str: jarFiles) {
                   jarFilesStr.append(LivyApp.uploadLocalFile(str)).append(",");
               }
            } catch (Exception e) {
                e.printStackTrace();
            }
            jarFilesStr.deleteCharAt(jarFilesStr.length() - 1);
            confMap.replace("spark.yarn.dist.jars", jarFilesStr.toString());
        }
        if(confMap.containsKey("spark.jars")) {
            confMap.remove("spark.jars");
        }
        if(confMap.get("spark.yarn.dist.files") != null){
            confMap.put("spark.yarn.dist.files", confMap.get("spark.yarn.dist.files")
                    + "," + properties.getProperty("hive-site.xml"));
        } else {
            confMap.put("spark.yarn.dist.files", properties.getProperty("hive-site.xml"));
        }
        if(properties.getProperty("rangerfiles") != null && properties.getProperty("rangerfiles").trim().length() > 0) {
            confMap.put("spark.yarn.dist.files", confMap.get("spark.yarn.dist.files") + "," + properties.getProperty("rangerfiles"));
        }
    }
}