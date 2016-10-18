import java.io.{BufferedReader, ByteArrayOutputStream, InputStreamReader, PrintStream}
import java.net.{URL, URLClassLoader}
import java.nio.charset.Charset
import java.util.concurrent.ExecutionException

import org.apache.spark.SparkContext
import org.apache.spark.repl.{SparkCommandLine, SparkIMain, SparkJLineCompletion}
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.concurrent.{Await, Future}
import scala.language.reflectiveCalls
import scala.tools.nsc.backend.JavaPlatform
import scala.tools.nsc.interpreter._
import scala.tools.nsc.io.AbstractFile
import scala.tools.nsc.util.{ClassPath, MergedClassPath}
import scala.tools.nsc.{Global, Settings, io}
import scala.util.{Try => UtilTry}

/**
  * Provides Scala version-specific features needed for the interpreter.
  */
class ScalaInterpreterSpecific {
  private val ExecutionExceptionName = "lastException"

  protected val logger = LoggerFactory.getLogger(this.getClass.getName)

  private val _thisClassloader = this.getClass.getClassLoader

  var sparkIMain: SparkIMain = _
  protected var jLineCompleter: SparkJLineCompletion = _

  private val outputStream = new ByteArrayOutputStream()

  val _runtimeClassloader =
    new URLClassLoader(Array(), _thisClassloader) {
      def addJar(url: URL) = this.addURL(url)
    }

  protected def newSparkIMain(
    settings: Settings, out: JPrintWriter
  ): SparkIMain = {
    val s = new SparkIMain(settings, out)
    s.initializeSynchronous()
    s
  }

  /** Since the ScalaInterpreter can be started without a kernel, we need to ensure that we can compile things.
    *Adding in the default classpaths as needed.
    */
  def appendClassPath(settings: Settings): Settings = {
    settings.classpath.value = buildClasspath(_thisClassloader)
    settings.embeddedDefaults(_runtimeClassloader)
    settings
  }

  protected def buildClasspath(classLoader: ClassLoader): String = {

    def toClassLoaderList( classLoader: ClassLoader ): Seq[ClassLoader] = {
      @tailrec
      def toClassLoaderListHelper( aClassLoader: ClassLoader, theList: Seq[ClassLoader]):Seq[ClassLoader] = {
        if( aClassLoader == null )
          return theList

        toClassLoaderListHelper( aClassLoader.getParent, aClassLoader +: theList )
      }
      toClassLoaderListHelper(classLoader, Seq())
    }

    val urls = toClassLoaderList(classLoader).flatMap{
      case cl: java.net.URLClassLoader => cl.getURLs.toList
      case a => List()
    }

    urls.foldLeft("")((l, r) => ClassPath.join(l, r.toString))
  }

  protected var settings: Settings = newSettings(List())
  settings = appendClassPath(settings)

  /**
    * Adds jars to the runtime and compile time classpaths. Does not work with
    * directories or expanding star in a path.
 *
    * @param jars The list of jar locations
    */
  def addJars(jars: URL*): Unit = {
    // Enable Scala class support
    reinitializeSymbols()

    jars.foreach(_runtimeClassloader.addJar)
    updateCompilerClassPath(jars : _*)

    // Refresh all of our variables
    refreshDefinitions()
  }

  // TODO: Need to figure out a better way to compare the representation of
  //       an annotation (contained in AnnotationInfo) with various annotations
  //       like scala.transient
  protected def convertAnnotationsToModifiers(
    annotationInfos: List[Global#AnnotationInfo]
  ) = annotationInfos map {
    case a if a.toString == "transient" => "@transient"
    case a =>
      logger.debug(s"Ignoring unknown annotation: $a")
      ""
  } filterNot {
        _.isEmpty
      }

  protected def convertScopeToModifiers(scopeSymbol: Global#Symbol) = {
    (if (scopeSymbol.isImplicit) "implicit" else "") ::
    Nil
  }

  protected def buildModifierList(termNameString: String) = {
    import scala.language.existentials
    val termSymbol = sparkIMain.symbolOfTerm(termNameString)


    convertAnnotationsToModifiers(
      if (termSymbol.hasAccessorFlag) termSymbol.accessed.annotations
      else termSymbol.annotations
    ) ++ convertScopeToModifiers(termSymbol)
  }

  protected def refreshDefinitions(): Unit = {
    sparkIMain.definedTerms.foreach(termName => {
      val termNameString = termName.toString
      val termTypeString = sparkIMain.typeOfTerm(termNameString).toLongString
      sparkIMain.valueOfTerm(termNameString) match {
        case Some(termValue)  =>
          val modifiers = buildModifierList(termNameString)
          logger.debug(s"Rebinding of $termNameString as " +
                       s"${modifiers.mkString(" ")} $termTypeString")
          UtilTry(sparkIMain.beSilentDuring {
            sparkIMain.bind(
              termNameString, termTypeString, termValue, modifiers
            )
          })
        case None             =>
          logger.debug(s"Ignoring rebinding of $termNameString")
      }
    })
  }

  protected def reinitializeSymbols(): Unit = {
    val global = sparkIMain.global
    import global._
    new Run // Initializes something needed for Scala classes
  }

  protected def updateCompilerClassPath( jars: URL*): Unit = {
    require(!sparkIMain.global.forMSIL) // Only support JavaPlatform

    val platform = sparkIMain.global.platform.asInstanceOf[JavaPlatform]

    val newClassPath = mergeJarsIntoClassPath(platform, jars:_*)
    logger.debug(s"newClassPath: ${newClassPath}")

    // TODO: Investigate better way to set this... one thought is to provide
    //       a classpath in the currentClassPath (which is merged) that can be
    //       replaced using updateClasspath, but would that work more than once?
    val fieldSetter = platform.getClass.getMethods
      .find(_.getName.endsWith("currentClassPath_$eq")).get
    fieldSetter.invoke(platform, Some(newClassPath))

    // Reload all jars specified into our compiler
    sparkIMain.global.invalidateClassPathEntries(jars.map(_.getPath): _*)
  }

  protected def mergeJarsIntoClassPath(platform: JavaPlatform, jars: URL*): MergedClassPath[AbstractFile] = {
    // Collect our new jars and add them to the existing set of classpaths
    val allClassPaths = (
      platform.classPath
        .asInstanceOf[MergedClassPath[AbstractFile]].entries
      ++
      jars.map(url =>
        platform.classPath.context.newClassPath(
          io.AbstractFile.getFile(url.getPath))
      )
      ).distinct

    // Combine all of our classpaths (old and new) into one merged classpath
    new MergedClassPath(
      allClassPaths,
      platform.classPath.context
    )
  }

  /**
    * Binds a variable in the interpreter to a value.
 *
    * @param variableName The name to expose the value in the interpreter
    * @param typeName The type of the variable, must be the fully qualified class name
    * @param value The value of the variable binding
    * @param modifiers Any annotation, scoping modifiers, etc on the variable
    */
  def bind(
    variableName: String,
    typeName: String,
    value: Any,
    modifiers: List[String]
  ): Unit = {
    require(sparkIMain != null)
    sparkIMain.bind(variableName, typeName, value, modifiers)
  }

  /**
    * Executes body and will not print anything to the console during the execution
 *
    * @param body The function to execute
    * @tparam T The return type of body
    * @return The return value of body
    */
  def doQuietly[T](body: => T): T = {
    require(sparkIMain != null)
    sparkIMain.beQuietDuring[T](body)
  }

  /**
    * Stops the interpreter, removing any previous internal state.
 *
    * @return A reference to the interpreter
    */
  def stop(): ScalaInterpreterSpecific = {
    logger.info("Shutting down interpreter")

    // Erase our completer
    jLineCompleter = null

    // Close the entire interpreter (loses all state)
    if (sparkIMain != null) sparkIMain.close()
    sparkIMain = null

    this
  }

  /**
    * Returns the name of the variable created from the last execution.
 *
    * @return Some String name if a variable was created, otherwise None
    */
  def lastExecutionVariableName: Option[String] = {
    require(sparkIMain != null)

    // TODO: Get this API method changed back to public in Apache Spark
    val lastRequestMethod = classOf[SparkIMain].getDeclaredMethod("lastRequest")
    lastRequestMethod.setAccessible(true)

    val request =
      lastRequestMethod.invoke(sparkIMain).asInstanceOf[SparkIMain#Request]

    val mostRecentVariableName = sparkIMain.mostRecentVar

    request.definedNames.map(_.toString).find(_ == mostRecentVariableName)
  }

  /**
    * Mask the Console and System objects with our wrapper implementations
    * and dump the Console methods into the public namespace (similar to
    * the Predef approach).
 *
    * @param in The new input stream
    * @param out The new output stream
    * @param err The new error stream
    */
  def updatePrintStreams(
    in: InputStream,
    out: OutputStream,
    err: OutputStream
  ): Unit = {
    val inReader = new BufferedReader(new InputStreamReader(in))
    val outPrinter = new PrintStream(out)
    val errPrinter = new PrintStream(err)

    sparkIMain.beQuietDuring {
      sparkIMain.addImports("Console._")
    }
  }

  /**
    * Retrieves the contents of the variable with the provided name from the
    * interpreter.
 *
    * @param variableName The name of the variable whose contents to read
    * @return An option containing the variable contents or None if the
    *         variable does not exist
    */
  def read(variableName: String): Option[AnyRef] = {
    require(sparkIMain != null)
    val variable = sparkIMain.valueOfTerm(variableName)
    if (variable == null || variable.isEmpty) None
    else variable
  }

  /**
    * Starts the interpreter, initializing any internal state.
    * You must call init before running this function.
    *
    * @return A reference to the interpreter
    */
   def start(): ScalaInterpreterSpecific = {
    require(sparkIMain == null)



    logger.debug("Initializing task manager")

    sparkIMain =
      newSparkIMain(settings, new JPrintWriter(outputStream, true))


    //logger.debug("Initializing interpreter")
   // sparkIMain.initializeSynchronous()

    logger.debug("Initializing completer")
    jLineCompleter = new SparkJLineCompletion(sparkIMain)

    sparkIMain.beQuietDuring {
      //logger.info("Rerouting Console and System related input and output")
      //updatePrintStreams(System.in, multiOutputStream, multiOutputStream)

      //   ADD IMPORTS generates too many classes, client is responsible for adding import
      logger.debug("Adding org.apache.spark.SparkContext._ to imports")
      sparkIMain.addImports("org.apache.spark.SparkContext._")
    }

    this
  }

  /**
    * Attempts to perform code completion via the <TAB> command.
 *
    * @param code The current cell to complete
    * @param pos The cursor position
    * @return The cursor position and list of possible completions
    */
  def completion(code: String, pos: Int): (Int, List[String]) = {
    require(jLineCompleter != null)

    logger.debug(s"Attempting code completion for ${code}")
    val regex = """[0-9a-zA-Z._]+$""".r
    val parsedCode = (regex findAllIn code).mkString("")

    logger.debug(s"Attempting code completion for ${parsedCode}")
    val result = jLineCompleter.completer().complete(parsedCode, pos)

    (result.cursor, result.candidates)
  }

  protected def newSettings(args: List[String]): Settings = {
    new SparkCommandLine(args).settings
  }

  def interpret(code: String, silent: Boolean): IR.Result = {
    if (sparkIMain == null) throw new IllegalArgumentException("Cannot interpret on a stopped interpreter")


        if (silent) {
          sparkIMain.beSilentDuring {
            sparkIMain.interpret(code)
          }
        } else {
          sparkIMain.interpret(code)
        }

  }


}
