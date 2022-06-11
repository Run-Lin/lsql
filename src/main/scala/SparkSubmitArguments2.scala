import java.io.{PrintStream, ByteArrayOutputStream}
import java.lang.reflect.InvocationTargetException
import java.nio.charset.StandardCharsets
import scala.io.Source


class SparkSubmitArguments2 {

  private def printUsageAndExit(exitCode: Int, unknownParam: Any = null): Unit = {
    // scalastyle:off println
    val outStream = System.err
    if (unknownParam != null) {
      outStream.println("Unknown/unsupported param " + unknownParam)
    }
    val command = sys.env.get("_SPARK_CMD_USAGE").getOrElse(
      """Usage: spark-submit [options] <app jar | python file> [app arguments]
        |Usage: spark-submit --kill [submission ID] --master [spark://...]
        |Usage: spark-submit --status [submission ID] --master [spark://...]
        |Usage: spark-submit run-example [options] example-class [example args]""".stripMargin)
    outStream.println(command)

    val mem_mb = "1024"
    outStream.println(
      s"""
         |Options:
         |  --class CLASS_NAME          Your application's main class (for Java / Scala apps).
         |  --name NAME                 A name of your application.
         |  --jars JARS                 Comma-separated list of local jars to include on the driver
         |                              and executor classpaths.
         |  --packages                  Comma-separated list of maven coordinates of jars to include
         |                              on the driver and executor classpaths. Will search the local
         |                              maven repo, then maven central and any additional remote
         |                              repositories given by --repositories. The format for the
         |                              coordinates should be groupId:artifactId:version.
         |  --exclude-packages          Comma-separated list of groupId:artifactId, to exclude while
         |                              resolving the dependencies provided in --packages to avoid
         |                              dependency conflicts.
         |  --repositories              Comma-separated list of additional remote repositories to
         |                              search for the maven coordinates given with --packages.
         |  --py-files PY_FILES         Comma-separated list of .zip, .egg, or .py files to place
         |                              on the PYTHONPATH for Python apps.
         |  --files FILES               Comma-separated list of files to be placed in the working
         |                              directory of each executor. File paths of these files
         |                              in executors can be accessed via SparkFiles.get(fileName).
         |
         |  --conf PROP=VALUE           Arbitrary Spark configuration property.
         |  --properties-file FILE      Path to a file from which to load extra properties. If not
         |                              specified, this will look for conf/spark-defaults.conf.
         |
         |  --driver-memory MEM         Memory for driver (e.g. 1000M, 2G) (Default: ${mem_mb}M).
         |  --driver-java-options       Extra Java options to pass to the driver.
         |  --driver-library-path       Extra library path entries to pass to the driver.
         |  --driver-class-path         Extra class path entries to pass to the driver. Note that
         |                              jars added with --jars are automatically included in the
         |                              classpath.
         |
         |  --executor-memory MEM       Memory per executor (e.g. 1000M, 2G) (Default: 1G).
         |
         |  --proxy-user NAME           User to impersonate when submitting the application.
         |                              This argument does not work with --principal / --keytab.
         |
         |  --help, -h                  Show this help message and exit.
         |  --verbose, -v               Print additional debug output.
         |  --version,                  Print the version of current Spark.
         |
         | Spark standalone and YARN only:
         |  --executor-cores NUM        Number of cores per executor. (Default: 1 in YARN mode,
         |                              or all available cores on the worker in standalone mode)
         |
         | YARN-only:
         |  --driver-cores NUM          Number of cores used by the driver, only in cluster mode
         |                              (Default: 1).
         |  --queue QUEUE_NAME          The YARN queue to submit to (Default: "default").
         |  --num-executors NUM         Number of executors to launch (Default: 2).
         |                              If dynamic allocation is enabled, the initial number of
         |                              executors will be at least NUM.
         |  --archives ARCHIVES         Comma separated list of archives to be extracted into the
         |                              working directory of each executor.
         |  --principal PRINCIPAL       Principal to be used to login to KDC, while running on
         |                              secure HDFS.
         |  --keytab KEYTAB             The full path to the file that contains the keytab for the
         |                              principal specified above. This keytab will be copied to
         |                              the node running the Application Master via the Secure
         |                              Distributed Cache, for renewing the login tickets and the
         |                              delegation tokens periodically.
      """.stripMargin
    )

    if (this.isSqlShell("")) {
      outStream.println("CLI options:")
      outStream.println(getSqlShellOptions())
    }
    // scalastyle:on println
    System.exit(exitCode)
  }
  /**
    * Run the Spark SQL CLI main class with the "--help" option and catch its output. Then filter
    * the results to remove unwanted lines.
    *
    * Since the CLI will call `System.exit()`, we install a security manager to prevent that call
    * from working, and restore the original one afterwards.
    */
  private def getSqlShellOptions(): String = {
    val currentOut = System.out
    val currentErr = System.err
    val currentSm = System.getSecurityManager()
    try {
      val out = new ByteArrayOutputStream()
      val stream = new PrintStream(out)
      System.setOut(stream)
      System.setErr(stream)

      val sm = new SecurityManager() {
        override def checkExit(status: Int): Unit = {
          throw new SecurityException()
        }

        override def checkPermission(perm: java.security.Permission): Unit = {}
      }
      System.setSecurityManager(sm)

      try {
        this.classForName("").getMethod("main", classOf[Array[String]])
          .invoke(null, Array("--help"))
      } catch {
        case e: InvocationTargetException =>
          // Ignore SecurityException, since we throw it above.
          if (!e.getCause().isInstanceOf[SecurityException]) {
            throw e
          }
      }

      stream.flush()

      // Get the output and discard any unnecessary lines from it.
      Source.fromString(new String(out.toByteArray(), StandardCharsets.UTF_8)).getLines
        .filter { line =>
          !line.startsWith("log4j") && !line.startsWith("usage")
        }
        .mkString("\n")
    } finally {
      System.setSecurityManager(currentSm)
      System.setOut(currentOut)
      System.setErr(currentErr)
    }
  }
  // scalastyle:off classforname
  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className)
    // scalastyle:on classforname
  }
  /**
    * Return whether the given main class represents a sql shell.
    */
  def isSqlShell(mainClass: String): Boolean = {
    mainClass == "org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver"
  }
  
}
