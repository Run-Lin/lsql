package com.r.spark.repl.sql

import java.io.PrintStream
import java.util

import com.r.spark.repl.sql.common.Utils
import com.r.spark.repl.sql.parser.SparkSubmitArguments
import com.google.gson.GsonBuilder

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer


object LivySubmit {
  val printStream: PrintStream = System.err
  def main(args: Array[String]) {
    val appArgs = new SparkSubmitArguments(args)
    var ( sparkProps , _ , livy, sysProps, _) =  appArgs.prepareSubmitEnvironment(appArgs)
    //set livy parse parameters
    var classIndex = sparkProps.indexOf("--class")
    var mainClass  = ""
    // TODO:将部分spark参数解析成livy参数
    if(classIndex < sparkProps.size) {
      // 若传入--class参数，则解析
       mainClass = sparkProps(classIndex + 1)
    }

    val livyArrays:ArrayBuffer[String] = new ArrayBuffer[String];
    livy.foreach(par => {
      livyArrays += (par._1)
      livyArrays += (par._2)
    })
    //livy main parser
    val mainParser = new MainOptionParser()
    val requestParms = mainParser.parseArgs(livyArrays.toArray)
    var confMap = requestParms.get("conf").asInstanceOf[util.Map[String, String]]
    if (confMap == null) {
      confMap = new util.HashMap[String, String]
      requestParms.put("conf", confMap)
    }
    //add spark properties config
    confMap.putAll(sysProps.asJava)

    if (confMap.get("spark.yarn.dist.pyFiles") != null
      && confMap.get("spark.yarn.dist.pyFiles").length > 0) {
      var pyFiles: Array[String] = confMap.get("spark.yarn.dist.pyFiles").split(",")
      var hdfsPyFiles: util.List[String] = new util.ArrayList[String]()
      pyFiles.foreach((pyFile: String) => {
        hdfsPyFiles.add(LivyApp.uploadLocalFile(pyFile))
      })
      var hdfsPyFilesArray = new Array[String](hdfsPyFiles.size())
      //Array
      requestParms.put("pyFiles", hdfsPyFiles.toArray(hdfsPyFilesArray))
      confMap.remove("spark.yarn.dist.pyFiles")
    }

    if (confMap.get("spark.yarn.dist.files")!= null
      && confMap.get("spark.yarn.dist.files").length > 0) {
      var files:Array[String] = confMap.get("spark.yarn.dist.files").split(",")
      var hdfsFilesStr: StringBuilder = new StringBuilder
      files.foreach((file: String) => {
        hdfsFilesStr.append(LivyApp.uploadLocalFile(file)).append(",")
      })
      hdfsFilesStr.deleteCharAt(hdfsFilesStr.length - 1)
      confMap.replace("spark.yarn.dist.files", hdfsFilesStr.toString())
    }
    if(confMap.containsKey("spark.files")) {
      confMap.remove("spark.files")
    }

    if (confMap.get("spark.yarn.dist.jars") != null
      && confMap.get("spark.yarn.dist.jars").length > 0) {
      var jarFiles:Array[String] = confMap.get("spark.yarn.dist.jars").split(",")
      var jarFilesStr: StringBuilder = new StringBuilder
      jarFiles.foreach((jarFile: String) => {
        jarFilesStr.append(LivyApp.uploadLocalFile(jarFile)).append(",")
      })
      jarFilesStr.deleteCharAt(jarFilesStr.length - 1)
      confMap.replace("spark.yarn.dist.jars", jarFilesStr.toString())
    }
    if(confMap.containsKey("spark.jars")) {
      confMap.remove("spark.jars")
    }

    val properties = Utils.loadHDFSProperties()
    if(properties == null) {
      System.err.println("cannot load properties.")
      System.exit(1)
    }
    //add hive-site.xml config on cluster
    if(confMap.get("spark.yarn.dist.files")!=null){
        confMap.put("spark.yarn.dist.files",
          Seq(confMap.get("spark.yarn.dist.files"), properties.getProperty("hive-site.xml")).mkString(","))
    }else{
        //confMap.put("spark.yarn.dist.files","hdfs://DClusterNmg/admin/livy/hive/hive-site.xml")
        confMap.put("spark.yarn.dist.files", properties.getProperty("hive-site.xml"))
    }
    requestParms.put("className", mainClass)
    //日志输出
    if (appArgs.verbose) {
      val gson = new GsonBuilder().setPrettyPrinting.disableHtmlEscaping.create
      printStream.println(appArgs)
      printStream.println(s"------request Params:${gson.toJson(requestParms)}------")
      printStream.println(s"------request app's Params:${gson.toJson(appArgs.appArgs.asJava)}------")
    }
    if(appArgs.appArgs.size <= 0 ){
       appArgs.printUsageAndExit(0)
    } else {
      //添加main class
      val filePath = LivyApp.uploadLocalFile(appArgs.appArgs.head)
      requestParms.put("file",filePath);
      val batch = new LivyBatch(requestParms, appArgs.appArgs.slice(1,appArgs.appArgs.size).toArray)
      val ret: Int = batch.executor()
      System.exit(ret)
    }
  }
}
