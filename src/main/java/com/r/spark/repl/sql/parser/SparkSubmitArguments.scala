package com.r.spark.repl.sql.parser
import java.io.{FileOutputStream, File, ByteArrayOutputStream, PrintStream}
import java.lang.reflect.InvocationTargetException
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.jar.JarFile
import java.util.logging.Level
import java.util.{List => JList}

import com.r.spark.repl.sql.common.Utils
import com.google.common.io.Files
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map, ArrayBuffer, HashMap}
import scala.io.Source
import scala.util.matching.Regex

/**
  * Parses and encapsulates arguments from the spark-submit script.
  * The env argument is used for testing.
  */
class SparkSubmitArguments(args: Seq[String], env: scala.Predef.Map[String, String] = sys.env) extends SparkSubmitArgumentsParser {
  var executorMemory: String = null
  var executorCores: String = null
  var totalExecutorCores: String = null
  var propertiesFile: String = null
  var driverMemory: String = null
  var driverExtraClassPath: String = null
  var driverExtraLibraryPath: String = null
  var driverExtraJavaOptions: String = null
  var queue: String = null
  var tags: String = null
  var numExecutors: String = null
  var files: String = null
  var archives: String = null
  var mainClass: String = null
  var primaryResource: String = null
  var name: String = null
  var childArgs: ArrayBuffer[String] = new ArrayBuffer[String]()
  var jars: String = null
  var packages: String = null
  var repositories: String = null
  var ivyRepoPath: String = null
  var packagesExclusions: String = null
  var verbose: Boolean = false
  var isPython: Boolean = false
  var pyFiles: String = null
  var isR: Boolean = false
  var action: String = null
  val sparkProperties: HashMap[String, String] = new HashMap[String, String]()
  var proxyUser: String = null
  var principal: String = null
  var keytab: String = null
  var priority: String = null

  // Standalone cluster mode only
  var supervise: Boolean = false
  var driverCores: String = null
  var submissionToKill: String = null
  var submissionToRequestStatusFor: String = null
  var useRest: Boolean = true // used internally
  var printStream: PrintStream = System.err
  // livy parameters

  //livy options
  var password: String = null
  var url: String = null
  var kind: String = null

  val appArgs = new ArrayBuffer[String]()

  /** Default properties present in the currently defined defaults file. */
  lazy val defaultSparkProperties: HashMap[String, String] = {
    val defaultProperties = new HashMap[String, String]()
    // scalastyle:off println
    if (verbose) System.err.println(s"Using properties file: $propertiesFile")
    Option(propertiesFile).foreach { filename =>
      val properties = Utils.getPropertiesFromFile(filename)
      properties.foreach { case (k, v) =>
        defaultProperties(k) = v
      }
      // Property files may contain sensitive information, so redact before printing
      if (verbose) {
        Utils.redact(properties).foreach { case (k, v) =>
          System.err.println(s"Adding default property: $k=$v")
        }
      }
    }
    // scalastyle:on println
    defaultProperties
  }

  // Set parameters from command line arguments
  try {
    parse(args.asJava)
  } catch {
    case e: IllegalArgumentException =>
      System.err.println(e.getMessage())
      System.err.println("Run with --help for usage help or --verbose for debug output")
      System.exit(1)
  }
  // Populate `sparkProperties` map from properties file
  mergeDefaultSparkProperties()
  // Remove keys that don't start with "spark." from `sparkProperties`.
  ignoreNonSparkProperties()
  // Use `sparkProperties` map along with env vars to fill in any missing parameters
  loadEnvironmentArguments()

  //validateArguments()

  /**
    * Merge values from the default properties file with those specified through --conf.
    * When this is called, `sparkProperties` is already filled with configs from the latter.
    */
  private def mergeDefaultSparkProperties(): Unit = {
    // Use common defaults file, if not specified by user
    propertiesFile = Option(propertiesFile).getOrElse(Utils.getDefaultPropertiesFile(env))
    // Honor --conf before the defaults file
    defaultSparkProperties.foreach { case (k, v) =>
      if (!sparkProperties.contains(k)) {
        sparkProperties(k) = v
      }
    }
  }

  /**
    * Remove keys that don't start with "spark." from `sparkProperties`.
    */
  private def ignoreNonSparkProperties(): Unit = {
    sparkProperties.foreach { case (k, v) =>
      if (!k.startsWith("spark.") && !k.startsWith("livy.")) {
        sparkProperties -= k
        Utils.printWarning(s"Ignoring non-spark config property: $k=$v")
      }
    }
  }

  /**
    * Load arguments from environment variables, Spark properties etc.
    */
  private def loadEnvironmentArguments(): Unit = {
    driverExtraClassPath = Option(driverExtraClassPath)
      .orElse(sparkProperties.get("spark.driver.extraClassPath"))
      .orNull
    driverExtraJavaOptions = Option(driverExtraJavaOptions)
      .orElse(sparkProperties.get("spark.driver.extraJavaOptions"))
      .orNull
    driverExtraLibraryPath = Option(driverExtraLibraryPath)
      .orElse(sparkProperties.get("spark.driver.extraLibraryPath"))
      .orNull
    driverMemory = Option(driverMemory)
      .orElse(sparkProperties.get("spark.driver.memory"))
      .orElse(env.get("SPARK_DRIVER_MEMORY"))
      .orNull
    driverCores = Option(driverCores)
      .orElse(sparkProperties.get("spark.driver.cores"))
      .orNull
    executorMemory = Option(executorMemory)
      .orElse(sparkProperties.get("spark.executor.memory"))
      .orElse(env.get("SPARK_EXECUTOR_MEMORY"))
      .orNull
    executorCores = Option(executorCores)
      .orElse(sparkProperties.get("spark.executor.cores"))
      .orElse(env.get("SPARK_EXECUTOR_CORES"))
      .orNull
    totalExecutorCores = Option(totalExecutorCores)
      .orElse(sparkProperties.get("spark.cores.max"))
      .orNull
    name = Option(name).orElse(sparkProperties.get("spark.app.name")).orNull
    jars = Option(jars).orElse(sparkProperties.get("spark.jars")).orNull
    files = Option(files).orElse(sparkProperties.get("spark.files")).orNull
    ivyRepoPath = sparkProperties.get("spark.jars.ivy").orNull
    packages = Option(packages).orElse(sparkProperties.get("spark.jars.packages")).orNull
    packagesExclusions = Option(packagesExclusions)
      .orElse(sparkProperties.get("spark.jars.excludes")).orNull
    numExecutors = Option(numExecutors)
      .getOrElse(sparkProperties.get("spark.executor.instances").orNull)
    queue = Option(queue).orElse(sparkProperties.get("spark.yarn.queue")).orElse(env.get("SPARK_YARN_QUEUE")).orNull
    tags = Option(tags).orElse(sparkProperties.get("spark.yarn.tags")).orElse(env.get("SPARK_YARN_TAGS")).orNull
    keytab = Option(keytab).orElse(sparkProperties.get("spark.yarn.keytab")).orNull
    principal = Option(principal).orElse(sparkProperties.get("spark.yarn.principal")).orNull
    priority = Option(priority).orElse(sparkProperties.get("spark.yarn.priority")).orElse(env.get("SPARK_YARN_PRIORITY")).orNull

    // Try to set main class from JAR if no --class argument is given
    if (mainClass == null && !isPython && !isR && primaryResource != null) {
      val uri = new URI(primaryResource)
      val uriScheme = uri.getScheme()

      uriScheme match {
        case "file" =>
          try {
            val jar = new JarFile(uri.getPath)
            // Note that this might still return null if no main-class is set; we catch that later
            mainClass = jar.getManifest.getMainAttributes.getValue("Main-Class")
          } catch {
            case e: Exception =>
              Utils.printErrorAndExit(s"Cannot load main class from JAR $primaryResource")
          }
        case _ =>
          Utils.printErrorAndExit(
            s"Cannot load main class from JAR $primaryResource with URI $uriScheme. " +
              "Please specify a class through --class.")
      }
    }

    // 从name获取-> 环境变量中获取 -> 取mainClass的值 -> 空值
    name = Option(name).orElse(env.get("SPARK_YARN_APP_NAME")).orNull

    // Set name from main class if not given
    name = Option(name).orElse(Option(mainClass)).orNull
    if (name == null && primaryResource != null) {
      name = Utils.stripDirectory(primaryResource)
    }
  }

  /** Ensure that required fields exists. Call this only once all defaults are loaded. */
  private def validateArguments(): Unit = {
    action match {
      case "SUBMIT" => validateSubmitArguments()
      case "KILL" => validateKillArguments()
      case "REQUEST_STATUS" => validateStatusRequestArguments()
    }
  }
  private def validateSubmitArguments(): Unit = {
    if (args.length == 0) {
      printUsageAndExit(-1)
    }
    if (primaryResource == null) {
      Utils.printErrorAndExit("Must specify a primary resource (JAR or Python or R file)")
    }
    if (mainClass == null && Utils.isUserJar(primaryResource)) {
      Utils.printErrorAndExit("No main class set in JAR; please specify one with --class")
    }
    if (pyFiles != null && !isPython) {
      Utils.printErrorAndExit("--py-files given but primary resource is not a Python script")
    }
    if (proxyUser != null && principal != null) {
      Utils.printErrorAndExit("Only one of --proxy-user or --principal can be provided.")
    }
  }

  private def validateKillArguments(): Unit = {
    if (submissionToKill == null) {
      Utils.printErrorAndExit("Please specify a submission to kill.")
    }
  }

  private def validateStatusRequestArguments(): Unit = {
    if (submissionToRequestStatusFor == null) {
      Utils.printErrorAndExit("Please specify a submission to request status for.")
    }
  }

  override def toString: String = {
    s"""Parsed arguments:
        |  executorMemory          $executorMemory
        |  executorCores           $executorCores
        |  totalExecutorCores      $totalExecutorCores
        |  propertiesFile          $propertiesFile
        |  driverMemory            $driverMemory
        |  driverCores             $driverCores
        |  driverExtraClassPath    $driverExtraClassPath
        |  driverExtraLibraryPath  $driverExtraLibraryPath
        |  driverExtraJavaOptions  $driverExtraJavaOptions
        |  supervise               $supervise
        |  queue                   $queue
        |  numExecutors            $numExecutors
        |  files                   $files
        |  pyFiles                 $pyFiles
        |  archives                $archives
        |  mainClass               $mainClass
        |  primaryResource         $primaryResource
        |  priority                $priority
        |  name                    $name
        |  childArgs               [${childArgs.mkString(" ")}]
        |  jars                    $jars
        |  packages                $packages
        |  packagesExclusions      $packagesExclusions
        |  repositories            $repositories
        |  verbose                 $verbose
        |
    |Spark properties used, including those specified through
        | --conf and those from the properties file $propertiesFile:
        |${Utils.redact(sparkProperties).mkString("  ", "\n  ", "\n")}
    """.stripMargin
  }

  /** Fill in values by parsing user options. */
  override protected def handle(opt: String, value: String): Boolean = {
    opt match {
      case NAME =>
        name = value
      case CLASS =>
        mainClass = value

      case DEPLOY_MODE =>
        if (value != "client" && value != "cluster") {
          Utils.printErrorAndExit("--deploy-mode must be either \"client\" or \"cluster\"")
        }
      case NUM_EXECUTORS =>
        numExecutors = value

      case TOTAL_EXECUTOR_CORES =>
        totalExecutorCores = value

      case EXECUTOR_CORES =>
        executorCores = value

      case EXECUTOR_MEMORY =>
        executorMemory = value

      case DRIVER_MEMORY =>
        driverMemory = value

      case DRIVER_CORES =>
        driverCores = value

      case DRIVER_CLASS_PATH =>
        driverExtraClassPath = value

      case DRIVER_JAVA_OPTIONS =>
        driverExtraJavaOptions = value

      case DRIVER_LIBRARY_PATH =>
        driverExtraLibraryPath = value

      case PROPERTIES_FILE =>
        propertiesFile = value

      case KILL_SUBMISSION =>
        submissionToKill = value
        action = "KILL"

      case STATUS =>
        submissionToRequestStatusFor = value
        action = "STATUS"

      case SUPERVISE =>
        supervise = true

      case QUEUE =>
        queue = value

      case PRIORITY =>
        priority = value

      case FILES =>
        files = Utils.resolveURIs(value)

      case PY_FILES =>
        pyFiles = Utils.resolveURIs(value)

      case ARCHIVES =>
        archives = Utils.resolveURIs(value)

      case JARS =>
        jars = Utils.resolveURIs(value)

      case PACKAGES =>
        packages = value

      case PACKAGES_EXCLUDE =>
        packagesExclusions = value

      case REPOSITORIES =>
        repositories = value

      case CONF =>
        val (confName, confValue) = Utils.parseSparkConfProperty(value)
        sparkProperties(confName) = confValue

      case PROXY_USER =>
        proxyUser = value

      case PRINCIPAL =>
        principal = value

      case KEYTAB =>
        keytab = value

      case HELP =>
        printUsageAndExit(0)

      case VERBOSE =>
        verbose = true

      case VERSION =>
        Utils.printVersionAndExit()

      case USAGE_ERROR =>
        printUsageAndExit(1)

      case PASSWORD =>
        password = value

      case URL =>
        url = value

      case KIND =>
        kind = value

      case _ =>
        throw new IllegalArgumentException(s"Unexpected argument '$opt'.")
    }
    true
  }

  /**
    * Handle unrecognized command line options.
    *
    * The first unrecognized option is treated as the "primary resource". Everything else is
    * treated as application arguments.
    */
  //将没有解析出来的参数统一放到appArgs里面
  override protected def handleUnknown(opt: String): Boolean = {
    appArgs.+=(opt)
    true
  }

  override protected def handleExtraArgs(extra: JList[String]): Unit = {
    childArgs ++= extra.asScala
  }

  def printUsageAndExit(exitCode: Int, unknownParam: Any = null): Unit = {
    // scalastyle:off println
    val outStream = Utils.printStream
    if (unknownParam != null) {
      outStream.println("Unknown/unsupported param " + unknownParam)
    }
    val command = sys.env.get("_SPARK_CMD_USAGE").getOrElse(
      """Usage: spark-submit [options] <app jar | python file> [app arguments]
        |Usage: spark-submit --kill [submission ID] --master [spark://...]
        |Usage: spark-submit --status [submission ID] --master [spark://...]
        |Usage: spark-submit run-example [options] example-class [example args]""".stripMargin)
    outStream.println(command)

    val mem_mb = Utils.DEFAULT_DRIVER_MEM_MB
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
        | Spark standalone with cluster deploy mode only:
         |  --driver-cores NUM          Cores for driver (Default: 1).
         |
        | Spark standalone or Mesos with cluster deploy mode only:
         |  --supervise                 If given, restarts the driver on failure.
         |  --kill SUBMISSION_ID        If given, kills the driver specified.
         |  --status SUBMISSION_ID      If given, requests the status of the driver specified.
         |
        | Spark standalone and Mesos only:
         |  --total-executor-cores NUM  Total cores for all executors.
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

    if (Utils.isSqlShell(mainClass)) {
      outStream.println("CLI options:")
      outStream.println(getSqlShellOptions())
    }
    // scalastyle:on println

    Utils.exitFn(exitCode)
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
        Utils.classForName(mainClass).getMethod("main", classOf[Array[String]])
          .invoke(null, Array(HELP))
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
  /**
    * Prepare the environment for submitting an application.
    *
    * @param args the parsed SparkSubmitArguments used for environment preparation.
    * @param conf the Hadoop Configuration, this argument will only be set in unit test.
    * @return a 4-tuple:
    *        (1) the arguments for the child process,
    *        (2) a list of classpath entries for the child,
    *        (3) a map of system properties, and
    *        (4) the main class for the child
    *
    * Exposed for testing.
    */
  def prepareSubmitEnvironment(args: SparkSubmitArguments) : (Seq[String], Seq[String],
    scala.collection.Map[String, String],
    scala.collection.Map[String, String], String) = {
    // Return values
    val childArgs = new ArrayBuffer[String]()
    val childClasspath = new ArrayBuffer[String]()
    val sysProps = new HashMap[String, String]()
    val livy = new HashMap[String, String]()
    var childMainClass = ""
    // Resolve maven dependencies if there are any and add classpath to jars. Add them to py-files
    // too for packages that include Python code
    val exclusions: Seq[String] =
      if (!StringUtils.isBlank(args.packagesExclusions)) {
        args.packagesExclusions.split(",")
      } else {
        Nil
      }

    // Create the IvySettings, either load from file or build defaults
    val ivySettings = args.sparkProperties.get("spark.jars.ivySettings").map { ivySettingsFile =>
      Utils.loadIvySettings(ivySettingsFile, Option(args.repositories),
        Option(args.ivyRepoPath))
    }.getOrElse {
      Utils.buildIvySettings(Option(args.repositories), Option(args.ivyRepoPath))
    }

    val resolvedMavenCoordinates = Utils.resolveMavenCoordinates(args.packages,
      ivySettings, exclusions = exclusions)
    if (!StringUtils.isBlank(resolvedMavenCoordinates)) {
      args.jars = mergeFileLists(args.jars, resolvedMavenCoordinates)
      if (args.isPython) {
        args.pyFiles = mergeFileLists(args.pyFiles, resolvedMavenCoordinates)
      }
    }

    // In client mode, download remote files.
    var localPrimaryResource: String = null
    var localJars: String = null
    var localPyFiles: String = null
    var localFiles: String = null

    // Require all python files to be local, so we can add them to the PYTHONPATH
    // In YARN cluster mode, python files are distributed as regular files, which can be non-local.
    // In Mesos cluster mode, non-local python files are automatically downloaded by Mesos.
    if (args.isPython) {
      if (Utils.nonLocalPaths(args.primaryResource).nonEmpty) {
        Utils.printErrorAndExit(s"Only local python files are supported: ${args.primaryResource}")
      }
      val nonLocalPyFiles = Utils.nonLocalPaths(args.pyFiles).mkString(",")
      if (nonLocalPyFiles.nonEmpty) {
        Utils.printErrorAndExit(s"Only local additional python files are supported: $nonLocalPyFiles")
      }
    }

    // Require all R files to be local
    if (args.isR ) {
      if (Utils.nonLocalPaths(args.primaryResource).nonEmpty) {
        Utils.printErrorAndExit(s"Only local R files are supported: ${args.primaryResource}")
      }
    }


    // If we're running a python app, set the main class to our specific python runner
    if (args.isPython) {
      if (args.primaryResource == Utils.PYSPARK_SHELL) {
        args.mainClass = "org.apache.spark.api.python.PythonGatewayServer"
      } else {
        // If a python file is provided, add it to the child arguments and list of files to deploy.
        // Usage: PythonAppRunner <main python file> <extra python files> [app arguments]
        args.mainClass = "org.apache.spark.deploy.PythonRunner"
        args.childArgs = ArrayBuffer(localPrimaryResource, localPyFiles) ++ args.childArgs
      }
      if (localPyFiles != null) {
        sysProps("spark.submit.pyFiles") = localPyFiles
      }
    }

    // In YARN mode for an R app, add the SparkR package archive and the R package
    // archive containing all of the built R libraries to archives so that they can
    // be distributed with the job
    if (args.isR ) {
      val sparkRPackagePath = Utils.localSparkRPackagePath
      if (sparkRPackagePath.isEmpty) {
        Utils.printErrorAndExit("SPARK_HOME does not exist for R application in YARN mode.")
      }
      val sparkRPackageFile = new File(sparkRPackagePath.get, Utils.SPARKR_PACKAGE_ARCHIVE)
      if (!sparkRPackageFile.exists()) {
       Utils.printErrorAndExit(s"${Utils.SPARKR_PACKAGE_ARCHIVE} does not exist for R application in YARN mode.")
      }
      val sparkRPackageURI = Utils.resolveURI(sparkRPackageFile.getAbsolutePath).toString

      // Distribute the SparkR package.
      // Assigns a symbol link name "sparkr" to the shipped package.
      args.archives = mergeFileLists(args.archives, sparkRPackageURI + "#sparkr")

      // Distribute the R package archive containing all the built R packages.
      if (!Utils.rPackages.isEmpty) {
        val rPackageFile =
          Utils.zipRLibraries(new File(Utils.rPackages.get), Utils.R_PACKAGE_ARCHIVE)
        if (!rPackageFile.exists()) {
          Utils.printErrorAndExit("Failed to zip all the built R packages.")
        }

        val rPackageURI = Utils.resolveURI(rPackageFile.getAbsolutePath).toString
        // Assigns a symbol link name "rpkg" to the shipped package.
        args.archives = mergeFileLists(args.archives, rPackageURI + "#rpkg")
      }
    }

    // TODO: Support distributing R packages with standalone cluster
    if (args.isR && !Utils.rPackages.isEmpty) {
      Utils.printErrorAndExit("Distributing R packages with standalone cluster is not supported.")
    }

    // TODO: Support distributing R packages with mesos cluster
    if (args.isR && !Utils.rPackages.isEmpty) {
      Utils.printErrorAndExit("Distributing R packages with mesos cluster is not supported.")
    }

    if (args.isR) {
      // In yarn-cluster mode for an R app, add primary resource to files
      // that can be distributed with the job
      args.files = mergeFileLists(args.files, args.primaryResource)
    }

    // Special flag to avoid deprecation warnings at the client
    sysProps("SPARK_SUBMIT") = "true"

    // A list of rules to map each argument to system properties or command-line options in
    // each deploy mode; we iterate through these below
    val options = List[OptionAssigner](

      OptionAssigner(args.name, Utils.ALL_CLUSTER_MGRS, Utils.ALL_DEPLOY_MODES, sysProp = "spark.app.name"),
      OptionAssigner(args.ivyRepoPath, Utils.ALL_CLUSTER_MGRS, Utils.CLIENT, sysProp = "spark.jars.ivy"),
      OptionAssigner(args.driverMemory, Utils.ALL_CLUSTER_MGRS, Utils.CLIENT,
        sysProp = "spark.driver.memory"),
      OptionAssigner(args.driverExtraClassPath, Utils.ALL_CLUSTER_MGRS, Utils.ALL_DEPLOY_MODES,
        sysProp = "spark.driver.extraClassPath"),
      OptionAssigner(args.driverExtraJavaOptions, Utils.ALL_CLUSTER_MGRS, Utils.ALL_DEPLOY_MODES,
        sysProp = "spark.driver.extraJavaOptions"),
      OptionAssigner(args.driverExtraLibraryPath, Utils.ALL_CLUSTER_MGRS, Utils.ALL_DEPLOY_MODES,
        sysProp = "spark.driver.extraLibraryPath"),

      // Yarn only
      OptionAssigner(args.queue, Utils.YARN, Utils.ALL_DEPLOY_MODES, sysProp = "spark.yarn.queue"),
      OptionAssigner(args.tags, Utils.YARN, Utils.ALL_DEPLOY_MODES, sysProp = "spark.yarn.tags"),
      OptionAssigner(args.numExecutors, Utils.YARN, Utils.ALL_DEPLOY_MODES,
        sysProp = "spark.executor.instances"),
      OptionAssigner(args.pyFiles, Utils.YARN, Utils.ALL_DEPLOY_MODES, sysProp = "spark.yarn.dist.pyFiles"),
      OptionAssigner(args.jars, Utils.YARN, Utils.ALL_DEPLOY_MODES, sysProp = "spark.yarn.dist.jars"),
      OptionAssigner(args.files, Utils.YARN, Utils.ALL_DEPLOY_MODES, sysProp = "spark.yarn.dist.files"),
      OptionAssigner(args.archives, Utils.YARN, Utils.ALL_DEPLOY_MODES, sysProp = "spark.yarn.dist.archives"),
      OptionAssigner(args.priority, Utils.YARN, Utils.ALL_DEPLOY_MODES, sysProp = "spark.yarn.priority"),
      OptionAssigner(args.principal, Utils.YARN, Utils.ALL_DEPLOY_MODES, sysProp = "spark.yarn.principal"),
      OptionAssigner(args.keytab, Utils.YARN, Utils.ALL_DEPLOY_MODES, sysProp = "spark.yarn.keytab"),

      // Other options
      OptionAssigner(args.executorCores, Utils.STANDALONE | Utils.YARN, Utils.ALL_DEPLOY_MODES,
        sysProp = "spark.executor.cores"),
      OptionAssigner(args.executorMemory, Utils.STANDALONE | Utils.MESOS | Utils.YARN, Utils.ALL_DEPLOY_MODES,
        sysProp = "spark.executor.memory"),
      OptionAssigner(args.totalExecutorCores, Utils.STANDALONE | Utils.MESOS, Utils.ALL_DEPLOY_MODES,
        sysProp = "spark.cores.max"),
      OptionAssigner(args.files, Utils.LOCAL | Utils.STANDALONE | Utils.MESOS, Utils.ALL_DEPLOY_MODES,
        sysProp = "spark.files"),
      OptionAssigner(args.jars, Utils.LOCAL, Utils.CLIENT, sysProp = "spark.jars"),
      OptionAssigner(args.jars, Utils.STANDALONE | Utils.MESOS, Utils.ALL_DEPLOY_MODES, sysProp = "spark.jars"),
      OptionAssigner(args.driverMemory, Utils.STANDALONE | Utils.MESOS | Utils.YARN, Utils.CLUSTER,
        sysProp = "spark.driver.memory"),
      OptionAssigner(args.driverCores, Utils.STANDALONE | Utils.MESOS | Utils.YARN, Utils.CLUSTER,
        sysProp = "spark.driver.cores"),
      OptionAssigner(args.supervise.toString, Utils.STANDALONE | Utils.MESOS, Utils.CLUSTER,
        sysProp = "spark.driver.supervise"),
      OptionAssigner(args.ivyRepoPath, Utils.STANDALONE, Utils.CLUSTER, sysProp = "spark.jars.ivy"),

      // An internal option used only for spark-shell to add user jars to repl's classloader,
      // previously it uses "spark.jars" or "spark.yarn.dist.jars" which now may be pointed to
      // remote jars, so adding a new option to only specify local jars for spark-shell internally.
      OptionAssigner(localJars, Utils.ALL_CLUSTER_MGRS, Utils.CLIENT, sysProp = "spark.repl.local.jars")
    )

    // Add the main application jar and any added jars to classpath in case YARN client
    // requires these jars.
    // This assumes both primaryResource and user jars are local jars, otherwise it will not be
    // added to the classpath of YARN client.
    if (isUserJar(args.primaryResource)) {
      childClasspath += args.primaryResource
    }
    if (args.jars != null) { childClasspath ++= args.jars.split(",") }

    if (args.childArgs != null) { childArgs ++= args.childArgs }

    // Map all arguments to command-line options or system properties for our chosen mode
    for (opt <- options) {
      if (opt.value != null) {
        if (opt.clOption != null) { childArgs += (opt.clOption, opt.value) }
        if (opt.sysProp != null) { sysProps.put(opt.sysProp, opt.value) }
      }
    }

    // Add the application jar automatically so the user doesn't have to call sc.addJar
    // For YARN cluster mode, the jar is already distributed on each node as "app.jar"
    // For python and R files, the primary resource is already distributed as a regular file
    if (!args.isPython && !args.isR) {
      var jars = sysProps.get("spark.jars").map(x => x.split(",").toSeq).getOrElse(Seq.empty)
      if (args.primaryResource!=null && isUserJar(args.primaryResource)) {
        jars = jars ++ Seq(args.primaryResource)
      }
      sysProps.put("spark.jars", jars.mkString(","))
    }

    if (args.isPython) {
      sysProps.put("spark.yarn.isPython", "true")
    }

    if (args.principal != null) {
      require(args.keytab != null, "Keytab must be specified when principal is specified")
      if (!new File(args.keytab).exists()) {
        throw new Exception(s"Keytab file: ${args.keytab} does not exist")
      } else {
        // Add keytab and principal configurations in sysProps to make them available
        // for later use; e.g. in spark sql, the isolated class loader used to talk
        // to HiveMetastore will use these settings. They will be set as Java system
        // properties and then loaded by SparkConf
        sysProps.put("spark.yarn.keytab", args.keytab)
        sysProps.put("spark.yarn.principal", args.principal)

        UserGroupInformation.loginUserFromKeytab(args.principal, args.keytab)
      }
    }

    childMainClass = "org.apache.spark.deploy.yarn.Client"
    if (args.isPython) {
      childArgs += ("--primary-py-file", args.primaryResource)
      childArgs += ("--class", "org.apache.spark.deploy.PythonRunner")
    } else if (args.isR) {
      val mainFile = new Path(args.primaryResource).getName
      childArgs += ("--primary-r-file", mainFile)
      childArgs += ("--class", "org.apache.spark.deploy.RRunner")
    } else {
      if (args.primaryResource != "spark-internal") {
        childArgs += ("--jar", args.primaryResource)
      }
      childArgs += ("--class", args.mainClass)
    }
    if (args.childArgs != null) {
      args.childArgs.foreach { arg => childArgs += ("--arg", arg) }
    }

    // Load any properties specified through --conf and the default properties file
    for ((k, v) <- args.sparkProperties) {
      sysProps.getOrElseUpdate(k, v)
    }

    // Ignore invalid spark.driver.host in cluster modes.
    sysProps -= "spark.driver.host"

    // Resolve paths in certain spark properties
    val pathConfigs = Seq(
      "spark.jars",
      "spark.files",
      "spark.yarn.dist.files",
      "spark.yarn.dist.archives",
      "spark.yarn.dist.jars")
    pathConfigs.foreach { config =>
      // Replace old URIs with resolved URIs, if they exist
      sysProps.get(config).foreach { oldValue =>
        sysProps(config) = Utils.resolveURIs(oldValue)
      }
    }

    // Resolve and format python file paths properly before adding them to the PYTHONPATH.
    // The resolving part is redundant in the case of --py-files, but necessary if the user
    // explicitly sets `spark.submit.pyFiles` in his/her default properties file.
    sysProps.get("spark.submit.pyFiles").foreach { pyFiles =>
      val resolvedPyFiles = Utils.resolveURIs(pyFiles)
      sysProps("spark.submit.pyFiles") = resolvedPyFiles
    }

    //livy parameters
    if(args.password!=null){
      livy.put("--password",args.password)
    }
    if(args.kind!=null){
      livy.put("--kind",args.kind)
    }
    if(args.url!=null){
      livy.put("--url",args.url)
    }
    if(args.proxyUser!=null){
      livy.put("--proxyUser",args.proxyUser)
    }
    if(args.queue!=null){
      livy.put("--queue",args.queue)
    }
    (childArgs, childClasspath,livy,sysProps, childMainClass)
  }
  /**
    * Merge a sequence of comma-separated file lists, some of which may be null to indicate
    * no files, into a single comma-separated string.
    */
  private def mergeFileLists(lists: String*): String = {
    val merged = lists.filterNot(StringUtils.isBlank)
      .flatMap(_.split(","))
      .mkString(",")
    if (merged == "") null else merged
  }



  /** Return all non-local paths from a comma-separated list of paths. */
  def nonLocalPaths(paths: String, testWindows: Boolean = false): Array[String] = {
    val windows = Utils.isWindows || testWindows
    if (paths == null || paths.trim.isEmpty) {
      Array.empty
    } else {
      paths.split(",").filter { p =>
        val uri = Utils.resolveURI(p)
        Option(uri.getScheme).getOrElse("file") match {
          case Utils.windowsDrive(d) if windows => false
          case "local" | "file" => false
          case _ => true
        }
      }
    }
  }
  /**
    * Provides an indirection layer for passing arguments as system properties or flags to
    * the user's driver program or to downstream launcher tools.
    */
  private case class OptionAssigner(
                                     value: String,
                                     clusterManager: Int,
                                     deployMode: Int,
                                     clOption: String = null,
                                     sysProp: String = null)

  /**
    * Return whether the given primary resource represents a user jar.
    */
  def isUserJar(res: String): Boolean = {
    !Utils.isShell(res) && !Utils.isPython(res) && !Utils.isInternal(res) && !Utils.isR(res)
  }

  /**
    * Checks the manifest of the Jar whether there is any R source code bundled with it.
    * Exposed for testing.
    */
  def checkManifestForR(jar: JarFile): Boolean = {
    if (jar.getManifest == null) {
      return false
    }
    val manifest = jar.getManifest.getMainAttributes
    manifest.getValue(Utils.hasRPackage) != null && manifest.getValue(Utils.hasRPackage).trim == "true"
  }

  /**
    * Extracts the files under /R in the jar to a temporary directory for building.
    */
  private def extractRFolder(jar: JarFile, printStream: PrintStream, verbose: Boolean): File = {
    val tempDir = Utils.createTempDir(null)
    val jarEntries = jar.entries()
    while (jarEntries.hasMoreElements) {
      val entry = jarEntries.nextElement()
      val entryRIndex = entry.getName.indexOf(Utils.RJarEntries)
      if (entryRIndex > -1) {
        val entryPath = entry.getName.substring(entryRIndex)
        if (entry.isDirectory) {
          val dir = new File(tempDir, entryPath)
          if (verbose) {
            print(s"Creating directory: $dir", printStream)
          }
          dir.mkdirs
        } else {
          val inStream = jar.getInputStream(entry)
          val outPath = new File(tempDir, entryPath)
          Files.createParentDirs(outPath)
          val outStream = new FileOutputStream(outPath)
          if (verbose) {
            print(s"Extracting $entry to $outPath", printStream)
          }
          Utils.copyStream(inStream, outStream, closeStreams = true)
        }
      }
    }
    tempDir
  }
}