package com.r.spark.repl.sql

import java.io.PrintStream
import java.util
import java.util.Date

import com.r.spark.repl.sql.parser.SparkSubmitArguments
import com.google.gson.GsonBuilder

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

object LivyShell {
  val printStream: PrintStream = System.err
  def main(args: Array[String]) {
    val appArgs = new SparkSubmitArguments(args)
    val ( _ , _ , livy, sysProps, _) =  appArgs.prepareSubmitEnvironment(appArgs)
    //set livy parse parameters
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
    //强制设置sql解释器
    requestParms.put("kind","spark")
    //日志输出
    if (appArgs.verbose) {
      val gson = new GsonBuilder().setPrettyPrinting.disableHtmlEscaping.create
      printStream.println(appArgs)
      printStream.println(s"------request Params:${gson.toJson(requestParms)}------")
      printStream.println(s"------request app's Params:${gson.toJson(appArgs.appArgs.asJava)}------")
    }
    val cli = new LivyCli(requestParms,appArgs.appArgs.asJava)
    val pingThread = new Thread() {
      override def run(): Unit = {
        try {
          while(true) {
            if (cli.interpreter != null) {
              cli.interpreter.ping()
            }
            Thread.sleep(30 * 1000)
          }
        } catch {
          case ex: InterruptedException => printStream.println("heartbeat thread is finished")
        }
      }
    }
    pingThread.setDaemon(true)
    pingThread.start()
    val ret: Int = cli.executor();
    System.exit(ret)
  }
}
