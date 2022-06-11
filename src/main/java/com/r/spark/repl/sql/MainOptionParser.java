package com.r.spark.repl.sql;

import org.apache.commons.cli.*;
import org.springframework.util.StringUtils;

import java.util.*;


public class MainOptionParser {

    // 使用spark的help usage
    public void helpUsage(Options options) {
        HelpFormatter formater = new HelpFormatter();
        System.err.println("Usage: livy-shell -k <spark,sql,pyspark,or sparkr> -q <yarn.queue> -u <livy.url> ");
        formater.printHelp("Main", options);
        // 错误码
        System.exit(1);
    }
    public Map<String,Object> parseArgs(String[] args){
        Map<String,Object> request = new java.util.HashMap<String,Object>();
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        Option proxyUser = new Option("u","proxyUser",true,"Hadoop user name(default: HADOOP_USER_NAME from the environment)");
        options.addOption(proxyUser);
        Option passWord = new Option("p","password",true,"Hadoop user password (default: HADOOP_USER_PASSWORD from the environment)");
        options.addOption(passWord);
        Option queue  = new Option("q","queue",true,"Hadoop yarn queue (default: SPARK_YARN_QUEUE from the environment)");
        queue.setType(String.class);
        options.addOption(queue);
        Option url = new Option("U","url",true,"livy server http url default:http://livy:8099");
        options.addOption(url);
        Option file = new Option("f","file",true,"File containing the application to execute path");
        options.addOption(file);
        Option kind = new Option("k","kind",true,"Session kind (spark,sql,pyspark,or sparkr,default:sql)");
        options.addOption(kind);
        Option className = new Option("c","className",true,"Application Java/Spark main class");
        options.addOption(className);
        Option argOps = new Option("a","args",true,"Command line arguments for the application");
        argOps.setType(List.class);
        options.addOption(argOps);
        Option jars = new Option((String)null, "jars", true, "Comma-separated list of local jars to include on the driver and executor classpaths.");
        jars.setType(List.class);
        options.addOption(jars);
        Option pyFiles = new Option("P","pyFiles",true,"Python files to be useed in this session");
        pyFiles.setType(List.class);
        options.addOption(pyFiles);
        Option files = new Option("F","files",true,"fils to be used in this session list of strings");
        files.setType(List.class);
        options.addOption(files);
        Option driverMem = new Option("d","driverMemory",true,"Amount of memory to use for the driver process");
        options.addOption(driverMem);
        Option driverCores = new Option("c","driverCores",true,"Number of cores to use for the driver process");
        driverCores.setType(Integer.class);
        options.addOption(driverCores);
        Option executorCores = new Option("ec","executorCores",true,"Number of cores to use for the executor process");
        executorCores.setType(Integer.class);
        options.addOption(executorCores);
        Option executorMemory = new Option("E","executorMemory",true,"Amount of memory to use per executor process");
        options.addOption(executorMemory);
        Option numExecutors = new Option("n","numExecutors",true,"Number of executors to launch for this session");
        numExecutors.setType(Integer.class);
        options.addOption(numExecutors);
        Option archives = new Option("A","archives",true,"Archives to be used in this session");
        options.addOption(archives);
        Option name = new Option("n","name",true,"The name of this session");
        options.addOption(name);
        Option conf = Option.builder("C")
                .longOpt("conf")
                .argName("property=value" )
                .hasArgs()
                .valueSeparator()
                .type(Map.class)
                .numberOfArgs(2)
                .desc("Spark configuration properties" )
                .build();
        options.addOption(conf);
        Option headers = new Option("H","headers",true,"Customer http headers");
        options.addOption(headers);
        Option help = new Option("h","help",false,"show help");
        options.addOption(help);
        Option silent = new Option("s","silent",false,"batch submit job!");
        options.addOption(silent);
        // -e 'quoted-query-string'
        Option exec = new Option("e", "exec",  true, "SQL from command line");
        options.addOption(exec);
//        options.addOption(OptionBuilder
//                .hasArg()
//                .withArgName("exec")
//                .withDescription("hcat command given from command line")
//                .create('e'));
        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );
            if (line.hasOption("h")){
                helpUsage(options);
            }
            String userName = line.getOptionValue("proxyUser");
            if(StringUtils.isEmpty(userName)){
                String envUserName = System.getenv().get("HADOOP_USER_NAME");
                if(StringUtils.isEmpty(envUserName)){
                    System.err.println("Please set HADOOP_USER_NAME in the environment variable");
                    helpUsage(options);
                }else{
                    request.put("proxyUser",envUserName);
                }
            }
            String password = line.getOptionValue("password");
            if(StringUtils.isEmpty(password)){
                String envUserPwd = System.getenv().get("HADOOP_USER_PASSWORD");
                if(StringUtils.isEmpty(envUserPwd)){
                    System.err.println("Please set HADOOP_USER_PASSWORD in the environment variable");
                    helpUsage(options);
                }else{
                    Map<String, String> confMap = (Map<String, String>) request.get("conf");
                    if (confMap == null) {
                        confMap = new java.util.HashMap<String,String>();
                        request.put("conf", confMap);
                    }
                    confMap.put("livy.rsc.proxy-password", envUserPwd);
                }
            }

            String queueOpt = line.getOptionValue("queue");
            if(StringUtils.isEmpty(queueOpt)){
                String envQueueName = System.getenv().get("SPARK_YARN_QUEUE");
                if(StringUtils.isEmpty(envQueueName)){
                    System.err.println("Please set SPARK_YARN_QUEUE in the environment variable");
                    helpUsage(options);
                }else{
                    request.put("queue",envQueueName);
                }
            }

            String tags = line.getOptionValue("tags");
            if(StringUtils.isEmpty(tags)){
                tags = System.getenv().get("SPARK_YARN_TAGS");
                if(!StringUtils.isEmpty(tags)){
                    request.put("tag",tags);
                }
            }

            // 从传入参数中解析得到sql的过程：1. 解析-f / -e ，得到参数传入livy batch
            // 执行livy batch rest api,其中args传入解析好的sql,Mainclass传入
            // 若有多条sql，则在jar包里做解析
            // -e
            String execString = line.getOptionValue("exec");


            Option[] reqOps = line.getOptions();
            for(Option opt:reqOps) {
                if (opt.getLongOpt().equals("password")) {
                    Map<String, String> confMap = (Map<String, String>) request.get("conf");
                    if (confMap == null) {
                        confMap = new java.util.HashMap<String,String>();
                        request.put("conf", confMap);
                    }
                    confMap.put("livy.rsc.proxy-password", opt.getValue());
                } else if (opt.getType().equals(Map.class)) {
                    Properties confPro = line.getOptionProperties("conf");
                    Map<String, String> confMap = (Map<String, String>) request.get("conf");
                    if (confMap == null) {
                        confMap = new java.util.HashMap<String,String>();
                        request.put("conf", confMap);
                    }
                    confMap.putAll( this.getMapFromProperties(confPro));
                } else if (opt.getType().equals(List.class)){
                    ArrayList optValues = new ArrayList();
                    Collections.addAll(optValues, opt.getValue().split(","));
                    request.put(opt.getLongOpt(), optValues);
                } else{
                    request.put(opt.getLongOpt(),opt.getValue());
                }
            }
            //replase sql to sql2
            String kindOpt = line.getOptionValue("kind");
            if(StringUtils.isEmpty(kindOpt)){
                request.put("kind","sql2");
            }else{
                if(kindOpt.equalsIgnoreCase("sql")){
                    request.put("kind","sql2");
                }
            }
            //set default url
            String urlOpt = line.getOptionValue("url");
            if(StringUtils.isEmpty(urlOpt)){
                Map<String, String> env = System.getenv();
                if(env.containsKey("LIVY_URL")) {
                    request.put("url",env.getOrDefault("LIVY_URL", "http://livy:8089"));
                } else {
                    request.put("url","http://livy:8089");
                }
            }
        }
        catch( ParseException exp ) {
            System.err.println( "Unexpected exception:" + exp.getMessage()+",you can use -h show help");
            helpUsage(options);
        }
        request.remove("password");
        request.remove("tag");

        /*if(request.containsKey("file") && !request.containsKey("exec")){
            request.replace("file", "hdfs://DClusterNmg4/user/wangzhefeng/warehouse/jars/sparksql-on-livybatch-1.0-SNAPSHOT.jar");
        }*/

        if(request.containsKey("exec")) {
            request.remove("exec");
        }
        return request;
    }

    private Map<String, String> getMapFromProperties(Properties confPro) {
        Map<String,String> optMaps = new HashMap<String,String>();
        Enumeration e = confPro.propertyNames();
        while(e.hasMoreElements()) {
            String key = (String)e.nextElement();
            optMaps.put(key, confPro.getProperty(key));
        }
        return optMaps;
    }
}
