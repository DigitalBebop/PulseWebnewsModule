package net.digitalbebop.pulsetestmodule


import java.io.{FileInputStream, Reader}
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.text.SimpleDateFormat
import java.util.concurrent.Executors
import java.util.{Calendar, Properties}
import java.util.concurrent.atomic.AtomicLong
import javax.net.ssl._

import net.digitalbebop.ClientRequests.IndexRequest
import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.commons.io.IOUtils
import org.apache.commons.net.nntp.{NewsgroupInfo, NNTPClient}
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Await, Future}
import scala.util.Try
import scala.util.parsing.json.JSONObject

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
    Try(new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z").parse(str).getTime)
    .orElse(Try(new SimpleDateFormat("dd MMM yyyy HH:mm:ss Z").parse(str).getTime)).toOption

  def createMessage(indexData: String, channel: String, id: Long, headers: Map[String, String]): IndexRequest = {
    val builder = IndexRequest.newBuilder()
    builder.setIndexData(indexData.replaceAll("[\n\r]", " "));

    builder.setMetaTags(new JSONObject(Map(("format", "text"), ("channel", channel))).toString())
    headers.get("Date").foreach { dateStr =>
      parseDate(dateStr).foreach(builder.setTimestamp)
    }
    builder.setModuleId(channel + "-" + id.toString)
    builder.setModuleName(moduleName)
    builder.addTags("news")
    builder.addTags(channel)
    headers.get("From").map { from =>
        val splits = from.replaceAll("[\\(\\)\\<\\>\"]", "").toLowerCase.split(" ")
        splits.foreach(builder.addTags)
        val email = splits.last
        builder.setUsername(email.split("@").head.replace(")", ""))
    }
    builder.build()
  }

  def parseHeaders(reader: Reader): Try[Map[String, String]] = Try {
    val input = IOUtils.toString(reader)
    input.split("\n").map { line =>
      val splits = line.split(":", 2).take(2).map(_.trim)
      (splits(0), splits(1))
    }.toMap
  }

  def processGroup(group: NewsgroupInfo): Unit = {
    val client = new NNTPClient()
    client.setSocketFactory(getSocketFactory)
    client.connect(nntpServer, nntpPort)
    client.authenticate(username, password)

    client.selectNewsgroup(group.getNewsgroup)
    val first = group.getFirstArticleLong
    val last = group.getLastArticleLong
    val httpClient = HttpClients.createDefault()
    println(s"backfilling for ${group.getNewsgroup}")
    (first to last).foreach { index =>
      processMessage(client, httpClient, group.getNewsgroup, index)
    }
  }

  def processGroup(group: NewsgroupInfo, startTimestamp: Long): Unit = {
    val client = new NNTPClient()
    val httpClient = HttpClients.createDefault()
    client.setSocketFactory(getSocketFactory)
    client.connect(nntpServer, nntpPort)
    client.authenticate(username, password)

    client.selectNewsgroup(group.getNewsgroup)
    println(s"updating for ${group.getNewsgroup}")
    client.iterateArticleInfo(group.getFirstArticleLong, group.getLastArticleLong)
      .filter { article =>
        parseDate(article.getDate).getOrElse(System.currentTimeMillis()) >= startTimestamp
      }.foreach { article =>
        processMessage(client, httpClient, group.getNewsgroup, article.getArticleNumberLong)
      }
  }

  /**
   * Parses message and uploads it to the server
   */
  def processMessage(client: NNTPClient, httpClient: CloseableHttpClient, group: String, index: Long): Unit = {
    parseHeaders(client.retrieveArticleHeader(index)).map { headers =>
      val body = IOUtils.toString(client.retrieveArticleBody(index))
      val message = createMessage(body, group, index, headers)
      val post = new HttpPost(s"$apiServer/api/index")
      post.setEntity(new ByteArrayEntity(message.toByteArray))
      httpClient.execute(post)
      val amount = messagesProcessed.incrementAndGet()
      if (amount % 1000 == 0) {
        val timeDiff = System.currentTimeMillis() / 1000 - startTime
        println(s"processed $amount requests, ${(1.0 * amount) / timeDiff} requests/sec")
      }
    }.getOrElse {
      messagesErrored.incrementAndGet()
    }
  }

  def main(args: Array[String]): Unit = {

    val options = new Options()
    options.addOption("config", true, "the configuration file to use")
    options.addOption("since", true, "the epoch timestamp to backfill from, 0 for full backfill")

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

    val backfillFrom = if (cmd.hasOption("since")) {
      cmd.getOptionValue("since").toLong
    } else {
      0L
    }
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(backfillFrom)
    println(s"backfilling from ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(cal.getTime)}")


    val client = new NNTPClient()
    client.setSocketFactory(getSocketFactory)
    client.connect(nntpServer, nntpPort)
    if (!client.authenticate(username, password)) {
      println(s"could not auth for user $username\nexiting...")
      sys.exit(1)
    }

    startTime = System.currentTimeMillis() / 1000
    if (backfillFrom == 0) {
      val futures = client.listNewsgroups().map { group =>
        Future { processGroup(group) }
      }
      Await.ready(Future.sequence(futures.toList), Duration.Inf)
    } else {
      val futures = client.listNewsgroups().map(group => Future { processGroup(group, backfillFrom) })
      Await.ready(Future.sequence(futures.toList), Duration.Inf)
    }
    println(s"processed: $messagesProcessed messages in ${System.currentTimeMillis() / 1000 - startTime} seconds")
    println(s"${messagesErrored.get()} messaged were not processed due to errors")
  }
}
