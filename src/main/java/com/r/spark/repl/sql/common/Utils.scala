package com.r.spark.repl.sql.common

import java.io._
import java.net.{URI, URISyntaxException}
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.text.ParseException
import java.util.{UUID, Properties}
import java.util.logging.Level
import java.util.zip.{ZipEntry, ZipOutputStream}

import com.r.spark.repl.sql.parser.SparkBuildInfo
import com.google.common.io.ByteStreams
import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.SystemUtils
import org.apache.hadoop.util.ShutdownHookManager
import org.apache.ivy.Ivy
import org.apache.ivy.core.LogOptions
import org.apache.ivy.core.module.descriptor._
import org.apache.ivy.core.module.id.{ModuleId, ArtifactId, ModuleRevisionId}
import org.apache.ivy.core.report.ResolveReport
import org.apache.ivy.core.resolve.ResolveOptions
import org.apache.ivy.core.retrieve.RetrieveOptions
import org.apache.ivy.core.settings.IvySettings
import org.apache.ivy.core.settings.IvySettings
import org.apache.ivy.plugins.matcher.GlobPatternMatcher
import org.apache.ivy.plugins.repository.file.FileRepository
import org.apache.ivy.plugins.resolver.{FileSystemResolver, IBiblioResolver, ChainResolver}
import org.apache.spark.network.util.JavaUtils

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.util.matching.Regex

object Utils{


  // Special primary resource names that represent shells rather than application jars.
  val DEFAULT_DRIVER_MEM_MB = JavaUtils.DEFAULT_DRIVER_MEM_MB.toInt

  val SPARK_VERSION = SparkBuildInfo.spark_version
  val SPARK_BRANCH = SparkBuildInfo.spark_branch
  val SPARK_REVISION = SparkBuildInfo.spark_revision
  val SPARK_BUILD_USER = SparkBuildInfo.spark_build_user
  val SPARK_REPO_URL = SparkBuildInfo.spark_repo_url
  val SPARK_BUILD_DATE = SparkBuildInfo.spark_build_date
  /**
    * Whether the underlying operating system is Windows.
    */
  val isWindows = SystemUtils.IS_OS_WINDOWS

  /**
    * Whether the underlying operating system is Mac OS X.
    */
  val isMac = SystemUtils.IS_OS_MAC_OSX

  /**
    * Pattern for matching a Windows drive, which contains only a single alphabet character.
    */
  val windowsDrive = "([a-zA-Z])".r

  var rPackages: Option[String] = None

  //spark parser
  // Cluster managers
  val YARN = 1
  val STANDALONE = 2
  val MESOS = 4
  val LOCAL = 8
  val ALL_CLUSTER_MGRS = YARN | STANDALONE | MESOS | LOCAL

  // Deploy modes
  val CLIENT = 1
  val CLUSTER = 2
  val ALL_DEPLOY_MODES = CLIENT | CLUSTER

  // Special primary resource names that represent shells rather than application jars.
  val SPARK_SHELL = "spark-shell"
  val PYSPARK_SHELL = "pyspark-shell"
  val SPARKR_SHELL = "sparkr-shell"
  val SPARKR_PACKAGE_ARCHIVE = "sparkr.zip"
  val R_PACKAGE_ARCHIVE = "rpkg.zip"

  val CLASS_NOT_FOUND_EXIT_STATUS = 101

  val hasRPackage = "Spark-HasRPackage"

  val RJarEntries = "R/pkg"

  val MAX_DIR_CREATION_ATTEMPTS: Int = 10

  var properties: Properties = _




  /** Load properties present in the given file. */
  def getPropertiesFromFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile(), s"Properties file $file is not a normal file")

    val inReader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().asScala.map(
        k => (k, properties.getProperty(k).trim)).toMap
    } catch {
      case e: IOException =>
        throw new Exception(s"Failed when loading Spark properties from $filename", e)
    } finally {
      inReader.close()
    }
  }
  /**
    * Looks up the redaction regex from within the key value pairs and uses it to redact the rest
    * of the key value pairs. No care is taken to make sure the redaction property itself is not
    * redacted. So theoretically, the property itself could be configured to redact its own value
    * when printing.
    */
  def redact(kvs: Map[String, String]): Seq[(String, String)] = {
    val redactionPattern = kvs.getOrElse(
      "spark.redaction.regex",
      "(?i)secret|password"
    ).r
    redact(redactionPattern, kvs.toSeq)
  }
  def redact(redactionPattern: Regex, kvs: Seq[(String, String)]): Seq[(String, String)] = {
    // If the sensitive information regex matches with either the key or the value, redact the value
    // While the original intent was to only redact the value if the key matched with the regex,
    // we've found that especially in verbose mode, the value of the property may contain sensitive
    // information like so:
    // "sun.java.command":"org.apache.spark.deploy.SparkSubmit ... \
    // --conf spark.executorEnv.HADOOP_CREDSTORE_PASSWORD=secret_password ...
    //
    // And, in such cases, simply searching for the sensitive information regex in the key name is
    // not sufficient. The values themselves have to be searched as well and redacted if matched.
    // This does mean we may be accounting more false positives - for example, if the value of an
    // arbitrary property contained the term 'password', we may redact the value from the UI and
    // logs. In order to work around it, user would have to make the spark.redaction.regex property
    // more specific.
    kvs.map { case (key, value) =>
      redactionPattern.findFirstIn(key)
        .orElse(redactionPattern.findFirstIn(value))
        .map { _ => (key, "*********(redacted)") }
        .getOrElse((key, value))
    }
  }
  /** Return the path of the default Spark properties file. */
  def getDefaultPropertiesFile(env: Map[String, String] = sys.env): String = {
    env.get("SPARK_CONF_DIR")
      .orElse(env.get("SPARK_HOME").map { t => s"$t${File.separator}conf" })
      .map { t => new File(s"$t${File.separator}spark-defaults.conf")}
      .filter(_.isFile)
      .map(_.getAbsolutePath)
      .orNull
  }
  var printStream: PrintStream = System.err
  // Exposed for testing
  var exitFn: Int => Unit = (exitCode: Int) => System.exit(exitCode)
  def printWarning(str: String): Unit = printStream.println("Warning: " + str)
  def printErrorAndExit(str: String): Unit = {
    printStream.println("Error: " + str)
    printStream.println("Run with --help for usage help or --verbose for debug output")
    exitFn(1)
  }
  /**
    * Strip the directory from a path name
    */
  def stripDirectory(path: String): String = {
    new File(path).getName
  }
  /**
    * Indicates whether Spark is currently running unit tests.
    */
  def isTesting: Boolean = {
    sys.env.contains("SPARK_TESTING") || sys.props.contains("spark.testing")
  }
  /**
    * Return whether the given primary resource represents a user jar.
    */
  def isUserJar(res: String): Boolean = {
    !isShell(res) && !isPython(res) && !isInternal(res) && !isR(res)
  }

  /**
    * Return whether the given primary resource represents a shell.
    */
  def isShell(res: String): Boolean = {
    (res == SPARK_SHELL || res == PYSPARK_SHELL || res == SPARKR_SHELL)
  }

  /**
    * Return whether the given main class represents a sql shell.
    */
  def isSqlShell(mainClass: String): Boolean = {
    mainClass == "org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver"
  }

  /**
    * Return whether the given primary resource requires running python.
    */
  def isPython(res: String): Boolean = {
    res != null && res.endsWith(".py") || res == PYSPARK_SHELL
  }

  /**
    * Return whether the given primary resource requires running R.
    */
  def isR(res: String): Boolean = {
    res != null && res.endsWith(".R") || res == SPARKR_SHELL
  }

  def isInternal(res: String): Boolean = {
    res == "spark-internal"
  }

  /**
    * Return a well-formed URI for the file described by a user input string.
    *
    * If the supplied path does not contain a scheme, or is a relative path, it will be
    * converted into an absolute path with a file:// scheme.
    */
  def resolveURI(path: String): URI = {
    try {
      val uri = new URI(path)
      if (uri.getScheme() != null) {
        return uri
      }
      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      if (uri.getFragment() != null) {
        val absoluteURI = new File(uri.getPath()).getAbsoluteFile().toURI()
        return new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment())
      }
    } catch {
      case e: URISyntaxException =>
    }
    new File(path).getAbsoluteFile().toURI()
  }

  /** Resolve a comma-separated list of paths. */
  def resolveURIs(paths: String): String = {
    if (paths == null || paths.trim.isEmpty) {
      ""
    } else {
      paths.split(",").filter(_.trim.nonEmpty).map { p => Utils.resolveURI(p) }.mkString(",")
    }
  }

  // scalastyle:on println
  def parseSparkConfProperty(pair: String): (String, String) = {
    pair.split("=", 2).toSeq match {
      case Seq(k, v) => (k, v)
      case _ => printErrorAndExit(s"Spark config without '=': $pair")
        throw new Exception(s"Spark config without '=': $pair")
    }
  }
  // scalastyle:off println
  private[spark] def printVersionAndExit(): Unit = {
    printStream.println("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
                        """.format(SPARK_VERSION))
    printStream.println("Using Scala %s, %s, %s".format(
      scala.util.Properties.versionString, scala.util.Properties.javaVmName, scala.util.Properties.javaVersion))
    printStream.println("Branch %s".format(SPARK_BRANCH))
    printStream.println("Compiled by user %s on %s".format(SPARK_BUILD_USER, SPARK_BUILD_DATE))
    printStream.println("Revision %s".format(SPARK_REVISION))
    printStream.println("Url %s".format(SPARK_REPO_URL))
    printStream.println("Type --help for more information.")
    exitFn(0)
  }
  // scalastyle:on println
  // scalastyle:off classforname
  /** Preferred alternative to Class.forName(className) */
  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getContextOrSparkClassLoader)
    // scalastyle:on classforname
  }

  /**
    * Get the Context ClassLoader on this thread or, if not present, the ClassLoader that
    * loaded Spark.
    *
    * This should be used whenever passing a ClassLoader to Class.ForName or finding the currently
    * active loader when setting up ClassLoader delegation chains.
    */
  def getContextOrSparkClassLoader: ClassLoader =
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getSparkClassLoader)

  /**
    * Get the ClassLoader which loaded Spark.
    */
  def getSparkClassLoader: ClassLoader = getClass.getClassLoader

  def resolveMavenCoordinates(
                               coordinates: String,
                               ivySettings: IvySettings,
                               exclusions: Seq[String] = Nil,
                               isTest: Boolean = false): String = {
    if (coordinates == null || coordinates.trim.isEmpty) {
      ""
    } else {
      val sysOut = System.out
      try {
        // To prevent ivy from logging to system out
        System.setOut(printStream)
        val artifacts = extractMavenCoordinates(coordinates)
        // Directories for caching downloads through ivy and storing the jars when maven coordinates
        // are supplied to spark-submit
        val packagesDirectory: File = new File(ivySettings.getDefaultIvyUserDir, "jars")
        // scalastyle:off println
        printStream.println(
          s"Ivy Default Cache set to: ${ivySettings.getDefaultCache.getAbsolutePath}")
        printStream.println(s"The jars for the packages stored in: $packagesDirectory")
        // scalastyle:on println

        val ivy = Ivy.newInstance(ivySettings)
        // Set resolve options to download transitive dependencies as well
        val resolveOptions = new ResolveOptions
        resolveOptions.setTransitive(true)
        val retrieveOptions = new RetrieveOptions
        // Turn downloading and logging off for testing
        if (isTest) {
          resolveOptions.setDownload(false)
          resolveOptions.setLog(LogOptions.LOG_QUIET)
          retrieveOptions.setLog(LogOptions.LOG_QUIET)
        } else {
          resolveOptions.setDownload(true)
        }

        // Default configuration name for ivy
        val ivyConfName = "default"

        // A Module descriptor must be specified. Entries are dummy strings
        val md = getModuleDescriptor
        // clear ivy resolution from previous launches. The resolution file is usually at
        // ~/.ivy2/org.apache.spark-spark-submit-parent-default.xml. In between runs, this file
        // leads to confusion with Ivy when the files can no longer be found at the repository
        // declared in that file/
        val mdId = md.getModuleRevisionId
        val previousResolution = new File(ivySettings.getDefaultCache,
          s"${mdId.getOrganisation}-${mdId.getName}-$ivyConfName.xml")
        if (previousResolution.exists) previousResolution.delete

        md.setDefaultConf(ivyConfName)

        // Add exclusion rules for Spark and Scala Library
        addExclusionRules(ivySettings, ivyConfName, md)
        // add all supplied maven artifacts as dependencies
        addDependenciesToIvy(md, artifacts, ivyConfName)
        exclusions.foreach { e =>
          md.addExcludeRule(createExclusion(e + ":*", ivySettings, ivyConfName))
        }
        // resolve dependencies
        val rr: ResolveReport = ivy.resolve(md, resolveOptions)
        if (rr.hasError) {
          throw new RuntimeException(rr.getAllProblemMessages.toString)
        }
        // retrieve all resolved dependencies
        ivy.retrieve(rr.getModuleDescriptor.getModuleRevisionId,
          packagesDirectory.getAbsolutePath + File.separator +
            "[organization]_[artifact]-[revision].[ext]",
          retrieveOptions.setConfs(Array(ivyConfName)))
        resolveDependencyPaths(rr.getArtifacts.toArray, packagesDirectory)
      } finally {
        System.setOut(sysOut)
      }
    }
  }
  /**
    * Extracts maven coordinates from a comma-delimited string. Coordinates should be provided
    * in the format `groupId:artifactId:version` or `groupId/artifactId:version`.
    *
    * @param coordinates Comma-delimited string of maven coordinates
    * @return Sequence of Maven coordinates
    */
  def extractMavenCoordinates(coordinates: String): Seq[MavenCoordinate] = {
    coordinates.split(",").map { p =>
      val splits = p.replace("/", ":").split(":")
      require(splits.length == 3, s"Provided Maven Coordinates must be in the form " +
        s"'groupId:artifactId:version'. The coordinate provided is: $p")
      require(splits(0) != null && splits(0).trim.nonEmpty, s"The groupId cannot be null or " +
        s"be whitespace. The groupId provided is: ${splits(0)}")
      require(splits(1) != null && splits(1).trim.nonEmpty, s"The artifactId cannot be null or " +
        s"be whitespace. The artifactId provided is: ${splits(1)}")
      require(splits(2) != null && splits(2).trim.nonEmpty, s"The version cannot be null or " +
        s"be whitespace. The version provided is: ${splits(2)}")
      new MavenCoordinate(splits(0), splits(1), splits(2))
    }
  }
  /** A nice function to use in tests as well. Values are dummy strings. */
  def getModuleDescriptor: DefaultModuleDescriptor = DefaultModuleDescriptor.newDefaultInstance(
    ModuleRevisionId.newInstance("org.apache.spark", "spark-submit-parent", "1.0"))

  /** Add exclusion rules for dependencies already included in the spark-assembly */
  def addExclusionRules(
                         ivySettings: IvySettings,
                         ivyConfName: String,
                         md: DefaultModuleDescriptor): Unit = {
    // Add scala exclusion rule
    md.addExcludeRule(createExclusion("*:scala-library:*", ivySettings, ivyConfName))

    // We need to specify each component explicitly, otherwise we miss spark-streaming-kafka-0-8 and
    // other spark-streaming utility components. Underscore is there to differentiate between
    // spark-streaming_2.1x and spark-streaming-kafka-0-8-assembly_2.1x
    val components = Seq("catalyst_", "core_", "graphx_", "hive_", "mllib_", "repl_",
      "sql_", "streaming_", "yarn_", "network-common_", "network-shuffle_", "network-yarn_")

    components.foreach { comp =>
      md.addExcludeRule(createExclusion(s"org.apache.spark:spark-$comp*:*", ivySettings,
        ivyConfName))
    }
  }

  /** Adds the given maven coordinates to Ivy's module descriptor. */
  def addDependenciesToIvy(
                            md: DefaultModuleDescriptor,
                            artifacts: Seq[MavenCoordinate],
                            ivyConfName: String): Unit = {
    artifacts.foreach { mvn =>
      val ri = ModuleRevisionId.newInstance(mvn.groupId, mvn.artifactId, mvn.version)
      val dd = new DefaultDependencyDescriptor(ri, false, false)
      dd.addDependencyConfiguration(ivyConfName, ivyConfName + "(runtime)")
      // scalastyle:off println
      printStream.println(s"${dd.getDependencyId} added as a dependency")
      // scalastyle:on println
      md.addDependency(dd)
    }
  }
  /**
    * Output a comma-delimited list of paths for the downloaded jars to be added to the classpath
    * (will append to jars in SparkSubmit).
    *
    * @param artifacts Sequence of dependencies that were resolved and retrieved
    * @param cacheDirectory directory where jars are cached
    * @return a comma-delimited list of paths for the dependencies
    */
  def resolveDependencyPaths(
                              artifacts: Array[AnyRef],
                              cacheDirectory: File): String = {
    artifacts.map { artifactInfo =>
      val artifact = artifactInfo.asInstanceOf[Artifact].getModuleRevisionId
      cacheDirectory.getAbsolutePath + File.separator +
        s"${artifact.getOrganisation}_${artifact.getName}-${artifact.getRevision}.jar"
    }.mkString(",")
  }
  def createExclusion(
                                       coords: String,
                                       ivySettings: IvySettings,
                                       ivyConfName: String): ExcludeRule = {
    val c = extractMavenCoordinates(coords)(0)
    val id = new ArtifactId(new ModuleId(c.groupId, c.artifactId), "*", "*", "*")
    val rule = new DefaultExcludeRule(id, ivySettings.getMatcher("glob"), null)
    rule.addConfiguration(ivyConfName)
    rule
  }
  /**
    * Represents a Maven Coordinate
    *
    * @param groupId the groupId of the coordinate
    * @param artifactId the artifactId of the coordinate
    * @param version the version of the coordinate
    */
  case class MavenCoordinate(groupId: String, artifactId: String, version: String) {
    override def toString: String = s"$groupId:$artifactId:$version"
  }

  /**
    * Load Ivy settings from a given filename, using supplied resolvers
    *
    * @param settingsFile Path to Ivy settings file
    * @param remoteRepos Comma-delimited string of remote repositories other than maven central
    * @param ivyPath The path to the local ivy repository
    * @return An IvySettings object
    */
  def loadIvySettings(
                       settingsFile: String,
                       remoteRepos: Option[String],
                       ivyPath: Option[String]): IvySettings = {
    val file = new File(settingsFile)
    require(file.exists(), s"Ivy settings file $file does not exist")
    require(file.isFile(), s"Ivy settings file $file is not a normal file")
    val ivySettings: IvySettings = new IvySettings
    try {
      ivySettings.load(file)
    } catch {
      case e @ (_: IOException | _: ParseException) =>
        throw new Exception(s"Failed when loading Ivy settings from $settingsFile", e)
    }
    processIvyPathArg(ivySettings, ivyPath)
    processRemoteRepoArg(ivySettings, remoteRepos)
    ivySettings
  }
  /* Set ivy settings for location of cache, if option is supplied */
  private def processIvyPathArg(ivySettings: IvySettings, ivyPath: Option[String]): Unit = {
    ivyPath.filterNot(_.trim.isEmpty).foreach { alternateIvyDir =>
      ivySettings.setDefaultIvyUserDir(new File(alternateIvyDir))
      ivySettings.setDefaultCache(new File(alternateIvyDir, "cache"))
    }
  }

  /* Add any optional additional remote repositories */
  private def processRemoteRepoArg(ivySettings: IvySettings, remoteRepos: Option[String]): Unit = {
    remoteRepos.filterNot(_.trim.isEmpty).map(_.split(",")).foreach { repositoryList =>
      val cr = new ChainResolver
      cr.setName("user-list")

      // add current default resolver, if any
      Option(ivySettings.getDefaultResolver).foreach(cr.add)

      // add additional repositories, last resolution in chain takes precedence
      repositoryList.zipWithIndex.foreach { case (repo, i) =>
        val brr: IBiblioResolver = new IBiblioResolver
        brr.setM2compatible(true)
        brr.setUsepoms(true)
        brr.setRoot(repo)
        brr.setName(s"repo-${i + 1}")
        cr.add(brr)
        // scalastyle:off println
        printStream.println(s"$repo added as a remote repository with the name: ${brr.getName}")
        // scalastyle:on println
      }

      ivySettings.addResolver(cr)
      ivySettings.setDefaultResolver(cr.getName)
    }
  }

  /**
    * Build Ivy Settings using options with default resolvers
    *
    * @param remoteRepos Comma-delimited string of remote repositories other than maven central
    * @param ivyPath The path to the local ivy repository
    * @return An IvySettings object
    */
  def buildIvySettings(remoteRepos: Option[String], ivyPath: Option[String]): IvySettings = {
    val ivySettings: IvySettings = new IvySettings
    processIvyPathArg(ivySettings, ivyPath)

    // create a pattern matcher
    ivySettings.addMatcher(new GlobPatternMatcher)
    // create the dependency resolvers
    val repoResolver = createRepoResolvers(ivySettings.getDefaultIvyUserDir)
    ivySettings.addResolver(repoResolver)
    ivySettings.setDefaultResolver(repoResolver.getName)
    processRemoteRepoArg(ivySettings, remoteRepos)
    ivySettings
  }


  /**
    * Execute a block of code, then a finally block, but if exceptions happen in
    * the finally block, do not suppress the original exception.
    *
    * This is primarily an issue with `finally { out.close() }` blocks, where
    * close needs to be called to clean up `out`, but if an exception happened
    * in `out.write`, it's likely `out` may be corrupted and `out.close` will
    * fail as well. This would then suppress the original/likely more meaningful
    * exception from the original `out.write` call.
    */
  def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case t: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        originalThrowable = t
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable if (originalThrowable != null && originalThrowable != t) =>
          originalThrowable.addSuppressed(t)
          throw originalThrowable
      }
    }
  }
  /**
    * Extracts maven coordinates from a comma-delimited string
    *
    * @param defaultIvyUserDir The default user path for Ivy
    * @return A ChainResolver used by Ivy to search for and resolve dependencies.
    */
  def createRepoResolvers(defaultIvyUserDir: File): ChainResolver = {
    // We need a chain resolver if we want to check multiple repositories
    val cr = new ChainResolver
    cr.setName("spark-list")

    val localM2 = new IBiblioResolver
    localM2.setM2compatible(true)
    localM2.setRoot(m2Path.toURI.toString)
    localM2.setUsepoms(true)
    localM2.setName("local-m2-cache")
    cr.add(localM2)

    val localIvy = new FileSystemResolver
    val localIvyRoot = new File(defaultIvyUserDir, "local")
    localIvy.setLocal(true)
    localIvy.setRepository(new FileRepository(localIvyRoot))
    val ivyPattern = Seq(localIvyRoot.getAbsolutePath, "[organisation]", "[module]", "[revision]",
      "ivys", "ivy.xml").mkString(File.separator)
    localIvy.addIvyPattern(ivyPattern)
    val artifactPattern = Seq(localIvyRoot.getAbsolutePath, "[organisation]", "[module]",
      "[revision]", "[type]s", "[artifact](-[classifier]).[ext]").mkString(File.separator)
    localIvy.addArtifactPattern(artifactPattern)
    localIvy.setName("local-ivy-cache")
    cr.add(localIvy)

    // the biblio resolver resolves POM declared dependencies
    val br: IBiblioResolver = new IBiblioResolver
    br.setM2compatible(true)
    br.setUsepoms(true)
    br.setName("central")
    cr.add(br)

    val sp: IBiblioResolver = new IBiblioResolver
    sp.setM2compatible(true)
    sp.setUsepoms(true)
    sp.setRoot("http://dl.bintray.com/spark-packages/maven")
    sp.setName("spark-packages")
    cr.add(sp)
    cr
  }
  /** Path of the local Maven cache. */
  private[spark] def m2Path: File = {
    if (Utils.isTesting) {
      // test builds delete the maven cache, and this can cause flakiness
      new File("dummy", ".m2" + File.separator + "repository")
    } else {
      new File(System.getProperty("user.home"), ".m2" + File.separator + "repository")
    }
  }
  /** Return all non-local paths from a comma-separated list of paths. */
  def nonLocalPaths(paths: String, testWindows: Boolean = false): Array[String] = {
    val windows = isWindows || testWindows
    if (paths == null || paths.trim.isEmpty) {
      Array.empty
    } else {
      paths.split(",").filter { p =>
        val uri = resolveURI(p)
        Option(uri.getScheme).getOrElse("file") match {
          case windowsDrive(d) if windows => false
          case "local" | "file" => false
          case _ => true
        }
      }
    }
  }
  /**
    * Get the SparkR package path in the local spark distribution.
    */
  def localSparkRPackagePath: Option[String] = {
    val sparkHome = sys.env.get("SPARK_HOME").orElse(sys.props.get("spark.test.home"))
    sparkHome.map(
      Seq(_, "R", "lib").mkString(File.separator)
    )
  }
  /** Zips all the R libraries built for distribution to the cluster. */
  def zipRLibraries(dir: File, name: String): File = {
    val filesToBundle = listFilesRecursively(dir, Seq(".zip"))
    // create a zip file from scratch, do not append to existing file.
    val zipFile = new File(dir, name)
    val zipOutputStream = new ZipOutputStream(new FileOutputStream(zipFile, false))
    try {
      filesToBundle.foreach { file =>
        // Get the relative paths for proper naming in the ZIP file. Note that
        // we convert dir to URI to force / and then remove trailing / that show up for
        // directories because the separator should always be / for according to ZIP
        // specification and therefore `relPath` here should be, for example,
        // "/packageTest/def.R" or "/test.R".
        val relPath = file.toURI.toString.replaceFirst(dir.toURI.toString.stripSuffix("/"), "")
        val fis = new FileInputStream(file)
        val zipEntry = new ZipEntry(relPath)
        zipOutputStream.putNextEntry(zipEntry)
        ByteStreams.copy(fis, zipOutputStream)
        zipOutputStream.closeEntry()
        fis.close()
      }
    } finally {
      zipOutputStream.close()
    }
    zipFile
  }

  private def listFilesRecursively(dir: File, excludePatterns: Seq[String]): Set[File] = {
    if (!dir.exists()) {
      Set.empty[File]
    } else {
      if (dir.isDirectory) {
        val subDir = dir.listFiles(new FilenameFilter {
          override def accept(dir: File, name: String): Boolean = {
            !excludePatterns.map(name.contains).reduce(_ || _) // exclude files with given pattern
          }
        })
        subDir.flatMap(listFilesRecursively(_, excludePatterns)).toSet
      } else {
        Set(dir)
      }
    }
  }

  /**
    * Create a temporary directory inside the given parent directory. The directory will be
    * automatically deleted when the VM shuts down.
    */
  def createTempDir(
                     root: String = System.getProperty("java.io.tmpdir"),
                     namePrefix: String = "spark"): File = {
    val dir = createDirectory(root, namePrefix)
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
      override def run(): Unit = {
        FileUtils.deleteDirectory(dir)
      }
    }))
    dir
  }


  /**
    * Create a directory inside the given parent directory. The directory is guaranteed to be
    * newly created, and is not marked for automatic deletion.
    */
  def createDirectory(root: String, namePrefix: String = "spark"): File = {
    var attempts = 0
    val maxAttempts = Utils.MAX_DIR_CREATION_ATTEMPTS
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: SecurityException => dir = null; }
    }

    dir.getCanonicalFile
  }

  /**
    * Copy all data from an InputStream to an OutputStream. NIO way of file stream to file stream
    * copying is disabled by default unless explicitly set transferToEnabled as true,
    * the parameter transferToEnabled should be configured by spark.file.transferTo = [true|false].
    */
  def copyStream(
                  in: InputStream,
                  out: OutputStream,
                  closeStreams: Boolean = false,
                  transferToEnabled: Boolean = false): Long = {
    tryWithSafeFinally {
      if (in.isInstanceOf[FileInputStream] && out.isInstanceOf[FileOutputStream]
        && transferToEnabled) {
        // When both streams are File stream, use transferTo to improve copy performance.
        val inChannel = in.asInstanceOf[FileInputStream].getChannel()
        val outChannel = out.asInstanceOf[FileOutputStream].getChannel()
        val size = inChannel.size()
        copyFileStreamNIO(inChannel, outChannel, 0, size)
        size
      } else {
        var count = 0L
        val buf = new Array[Byte](8192)
        var n = 0
        while (n != -1) {
          n = in.read(buf)
          if (n != -1) {
            out.write(buf, 0, n)
            count += n
          }
        }
        count
      }
    } {
      if (closeStreams) {
        try {
          in.close()
        } finally {
          out.close()
        }
      }
    }
  }

  def copyFileStreamNIO(
                         input: FileChannel,
                         output: FileChannel,
                         startPosition: Long,
                         bytesToCopy: Long): Unit = {
    val initialPos = output.position()
    var count = 0L
    // In case transferTo method transferred less data than we have required.
    while (count < bytesToCopy) {
      count += input.transferTo(count + startPosition, bytesToCopy - count, output)
    }
    assert(count == bytesToCopy,
      s"request to copy $bytesToCopy bytes, but actually copied $count bytes.")

    // Check the position after transferTo loop to see if it is in the right position and
    // give user information if not.
    // Position will not be increased to the expected length after calling transferTo in
    // kernel version 2.6.32, this issue can be seen in
    // https://bugs.openjdk.java.net/browse/JDK-7052359
    // This will lead to stream corruption issue when using sort-based shuffle (SPARK-3948).
    val finalPos = output.position()
    val expectedPos = initialPos + bytesToCopy
    assert(finalPos == expectedPos,
      s"""
         |Current position $finalPos do not equal to expected position $expectedPos
         |after transferTo, please check your kernel version to see if it is 2.6.32,
         |this is a kernel bug which will lead to unexpected behavior when using transferTo.
         |You can set spark.file.transferTo = false to disable this NIO feature.
           """.stripMargin)
  }

  def loadHDFSProperties(): Properties = try {
    val inputStream = Thread.currentThread.getContextClassLoader.getResourceAsStream("hdfs.properties")
    if (properties == null) properties = new Properties
    properties.load(inputStream)
    properties
  } catch {
    case e: IOException =>
      e.printStackTrace()
      null
  }
}
