package com.r.spark.repl.sql.parser

import java.util.Properties

object SparkBuildInfo {

    val (
      spark_version: String,
      spark_branch: String,
      spark_revision: String,
      spark_build_user: String,
      spark_repo_url: String,
      spark_build_date: String) = {

      val resourceStream = Thread.currentThread().getContextClassLoader.
        getResourceAsStream("spark-version-info.properties")

      try {
        val unknownProp = "<unknown>"
        val props = new Properties()
        props.load(resourceStream)
        (
          props.getProperty("version", unknownProp),
          props.getProperty("branch", unknownProp),
          props.getProperty("revision", unknownProp),
          props.getProperty("user", unknownProp),
          props.getProperty("url", unknownProp),
          props.getProperty("date", unknownProp)
          )
      } catch {
        case npe: NullPointerException =>
          throw new Exception("Error while locating file spark-version-info.properties", npe)
        case e: Exception =>
          throw new Exception("Error loading properties from spark-version-info.properties", e)
      } finally {
        if (resourceStream != null) {
          try {
            resourceStream.close()
          } catch {
            case e: Exception =>
              throw new Exception("Error closing spark build info resource stream", e)
          }
        }
      }
    }
}

