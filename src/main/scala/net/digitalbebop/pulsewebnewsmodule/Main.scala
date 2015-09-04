package net.digitalbebop.pulsewebnewsmodule


import java.io._
import java.net.{HttpURLConnection, URL}
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.text.SimpleDateFormat
import java.util.concurrent.Executors
import java.util.{Calendar, Properties}
import java.util.concurrent.atomic.AtomicLong
import javax.net.ssl._

import com.google.gson.stream.JsonWriter
import com.google.protobuf.ByteString
import net.digitalbebop.ClientRequests.IndexRequest
import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.commons.io.IOUtils
import org.apache.commons.net.nntp.{NewsgroupInfo, NNTPClient}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext
import scala.util.Try

class NaiveTrustManager extends X509TrustManager {
  /**
   * Doesn't throw an exception, so this is how it approves a certificate.
   * @see javax.net.ssl.X509TrustManager#checkClientTrusted(java.security.cert.X509Certificate[], String)
   **/
  def checkClientTrusted(cert: Array[X509Certificate], authType: String): Unit = { }


  /**
   * Doesn't throw an exception, so this is how it approves a certificate.
   * @see javax.net.ssl.X509TrustManager#checkServerTrusted(java.security.cert.X509Certificate[], String)
   **/
  def checkServerTrusted (cert: Array[X509Certificate], authType: String): Unit = { }

  /**
   * @see javax.net.ssl.X509TrustManager#getAcceptedIssuers()
   **/
   def getAcceptedIssuers(): Array[X509Certificate] = Array()
}

object Main {

  final val WEBNEWS = "https://webnews.csh.rit.edu/#!"

  var username: String = _
  var password: String = _
  var nntpServer: String = _
  var nntpPort: Int = _
  var moduleName: String = _
  var apiServer: String = _

  var startTime: Long = _
  val messagesProcessed  = new AtomicLong(0)
  val messagesErrored = new AtomicLong(0)

  implicit val ec = new ExecutionContext {
    val threadPool = Executors.newCachedThreadPool()

    def execute(runnable: Runnable): Unit = threadPool.submit(runnable)

    def reportFailure(t: Throwable) {}
  }

  lazy val getSocketFactory: SSLSocketFactory = {
    val tm: Array[TrustManager] = Array(new NaiveTrustManager())
    val context = SSLContext.getInstance("SSL")
    context.init(Array[KeyManager](), tm, new SecureRandom())
    context.getSocketFactory
  }

  def parseDate(str: String): Option[Long] =
    Try(new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z").parse(str).getTime)
      .orElse(Try(new SimpleDateFormat("EEE, dd MMM yyyy HH:mm Z").parse(str).getTime))
      .orElse(Try(new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss").parse(str).getTime))
      .orElse(Try(new SimpleDateFormat("dd MMM yyyy HH:mm:ss Z").parse(str).getTime)).toOption

  def parseHeaders(reader: Reader): Try[Map[String, String]] = Try {
    val input = IOUtils.toString(reader)
    input.split("\n").flatMap { line =>
      if (line.contains(":")) {
        val splits = line.split(":", 2)
        if (splits.length > 2) {
          val splits2 = splits.take(2).map(_.trim)
          List((splits(0), splits(1)))
        } else {
          List.empty
        }
      } else {
        List.empty
      }
    }.toMap
  }

  def processGroup(group: NewsgroupInfo, writer: JsonWriter): Unit = {
    val client = new NNTPClient()
    client.setSocketFactory(getSocketFactory)
    client.connect(nntpServer, nntpPort)
    client.authenticate(username, password)

    client.selectNewsgroup(group.getNewsgroup)
    val first = group.getFirstArticleLong
    val last = group.getLastArticleLong
    println(s"backfilling for ${group.getNewsgroup}")
    (first to last).foreach { index =>
      processMessage(client, group.getNewsgroup, index, writer)
    }
  }

  /**
   * Parses message and uploads it to the server
   */
  def processMessage(client: NNTPClient, group: String, index: Long, writer: JsonWriter): Unit = {

    try {
      val headerReader = client.retrieveArticleHeader(index)
      val headers = IOUtils.toString(headerReader)
      var date: Long = 0
      var user: String = ""

      headers.split("\n").foreach { line =>
          if (line.startsWith("Date:")) {
            parseDate(line.drop(5).trim).foreach(d => date = d)
          } else if (line.startsWith("From:")) {
            user = line.drop(6).trim
          }
      }

      val body = IOUtils.toString(client.retrieveArticleBody(index))
      writer.beginObject()
        .name("group").value(group)
        .name("body").value(body)
        .name("date").value(date)
        .name("username").value(user)
        .endObject()
      val amount = messagesProcessed.incrementAndGet()
      if (amount % 1000 == 0) {
        val timeDiff = System.currentTimeMillis() / 1000 - startTime
        println(s"processed $amount requests, ${(1.0 * amount) / timeDiff} requests/sec")
      }
    } catch {
      case ex: Exception =>
    }
  }

  def isUp(url: String): Boolean = try {
    val con = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    con.setRequestMethod("HEAD")
    con.setConnectTimeout(5000) //set timeout to 5 seconds
    con.getResponseCode()
    true
  } catch {
    case ex: Exception => false
  }

  def main(args: Array[String]): Unit = {

    val options = new Options()
    options.addOption("config", true, "the configuration file to use")

    val parser = new DefaultParser()
    val cmd = parser.parse(options, args)

    if (cmd.hasOption("config")) {
      val file = cmd.getOptionValue("config")
      println(s"using configuration file: $file")
      val input = new FileInputStream(file)
      val props = new Properties()
      props.load(input)

      username = props.getProperty("username")
      password = props.getProperty("password")
      nntpServer = props.getProperty("nntpServer")
      nntpPort = props.getProperty("nntpPort").toInt
      moduleName = props.getProperty("moduleName")
      apiServer = props.getProperty("apiServer")

    } else {
      print("username: ")
      username = readLine()
      print("password: ")
      password = System.console().readPassword().mkString("")
      nntpServer = "news.csh.rit.edu"
      nntpPort = 563
      moduleName = "news"
      apiServer = "http://localhost:8080"
    }

    val client = new NNTPClient()
    client.setSocketFactory(getSocketFactory)
    client.connect(nntpServer, nntpPort)
    if (!client.authenticate(username, password)) {
      println(s"could not auth for user $username\nexiting...")
      sys.exit(1)
    }

    if (isUp(apiServer)) {
      startTime = System.currentTimeMillis() / 1000

      val writer = new JsonWriter(new OutputStreamWriter(new FileOutputStream("output.txt"), "UTF-8"))
      writer.setIndent(" ")
      writer.beginArray()

      client.listNewsgroups().foreach { group => processGroup(group, writer) }

      writer.endArray()
      writer.flush()
      writer.close()

      println(s"processed: $messagesProcessed messages in ${System.currentTimeMillis() / 1000 - startTime} seconds")
      println(s"${messagesErrored.get()} messaged were not processed due to errors")
    } else {
      println(s"server $apiServer does not seem to be up")
    }
  }
}
