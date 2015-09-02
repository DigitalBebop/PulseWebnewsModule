package net.digitalbebop.pulsewebnewsmodule


import java.io.{FileInputStream, Reader}
import java.net.{HttpURLConnection, URL}
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.text.SimpleDateFormat
import java.util.concurrent.{TimeUnit, Executors}
import java.util.{Calendar, Properties}
import java.util.concurrent.atomic.AtomicLong
import javax.net.ssl._

import com.google.protobuf.ByteString
import net.digitalbebop.ClientRequests.IndexRequest
import org.apache.commons.cli.{DefaultParser, Options}
import org.apache.commons.io.IOUtils
import org.apache.commons.net.nntp.{NewsgroupInfo, NNTPClient}
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.{HttpClientBuilder, DefaultHttpClient, HttpClients}
import org.apache.http.params.{BasicHttpParams, HttpConnectionParams}

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

  def createMessage(indexData: String, channel: String, id: Long, headers: Map[String, String]): IndexRequest = {
    val builder = IndexRequest.newBuilder()
    builder.setIndexData(indexData.replaceAll("[\n\r]", " "))
    builder.setRawData(ByteString.copyFrom(indexData.getBytes))

    builder.setMetaTags(new JSONObject(Map(("format", "text"), ("title", headers("Subject")), ("channel", channel))).toString())
    headers.get("Date").map { dateStr =>
      parseDate(dateStr).map(builder.setTimestamp).getOrElse(println("could not get date for: " + dateStr))
    }.getOrElse(println(headers))
    builder.setModuleId(channel + "-" + id.toString)
    builder.setModuleName(moduleName)
    builder.addTags("news")
    builder.addTags(channel)
    builder.setLocation(s"$WEBNEWS/$channel/$id")
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
    println(s"backfilling for ${group.getNewsgroup}")
    (first to last).foreach { index =>
      processMessage(client, group.getNewsgroup, index)
    }
  }

  def processGroup(group: NewsgroupInfo, startTimestamp: Long): Unit = {
    val client = new NNTPClient()
    client.setSocketFactory(getSocketFactory)
    client.connect(nntpServer, nntpPort)
    client.authenticate(username, password)

    client.selectNewsgroup(group.getNewsgroup)
    println(s"updating for ${group.getNewsgroup}")
    client.iterateArticleInfo(group.getFirstArticleLong, group.getLastArticleLong)
      .filter { article =>
        parseDate(article.getDate).getOrElse(System.currentTimeMillis()) >= startTimestamp
      }.foreach { article =>
        processMessage(client, group.getNewsgroup, article.getArticleNumberLong)
      }
  }

  /**
   * Parses message and uploads it to the server
   */
  def processMessage(client: NNTPClient, group: String, index: Long): Unit = {
    parseHeaders(client.retrieveArticleHeader(index)).map { headers =>
      val body = IOUtils.toString(client.retrieveArticleBody(index))
      if (!group.contains("test")) {
        val message = createMessage(body, group, index, headers)
        val post = new HttpPost(s"$apiServer/api/index")
        post.setEntity(new ByteArrayEntity(message.toByteArray))
        HttpClients.createDefault().execute(post).close()
        val amount = messagesProcessed.incrementAndGet()
        if (amount % 1000 == 0) {
          val timeDiff = System.currentTimeMillis() / 1000 - startTime
          println(s"processed $amount requests, ${(1.0 * amount) / timeDiff} requests/sec")
        }
      }
    }.getOrElse {
      messagesErrored.incrementAndGet()
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
    options.addOption("since", true, "the number of seconds to backfill for, 0 for full backfill")

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
    val timestamp = System.currentTimeMillis() - backfillFrom * 1000
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(timestamp)
    println(s"backfilling everything since ${new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(cal.getTime)}")


    val client = new NNTPClient()
    client.setSocketFactory(getSocketFactory)
    client.connect(nntpServer, nntpPort)
    if (!client.authenticate(username, password)) {
      println(s"could not auth for user $username\nexiting...")
      sys.exit(1)
    }

    if (isUp(apiServer)) {
      startTime = System.currentTimeMillis() / 1000
      if (backfillFrom == 0) {
        val futures = client.listNewsgroups().map { group =>
          Future {
            processGroup(group)
          }
        }
        Await.ready(Future.sequence(futures.toList), Duration.Inf)
      } else {
        val futures = client.listNewsgroups().map(group => Future {
          processGroup(group, timestamp)
        })
        Await.ready(Future.sequence(futures.toList), Duration.Inf)
      }
      ec.threadPool.shutdownNow()
      println(s"processed: $messagesProcessed messages in ${System.currentTimeMillis() / 1000 - startTime} seconds")
      println(s"${messagesErrored.get()} messaged were not processed due to errors")
    } else {
      println(s"server $apiServer does not seem to be up")
    }
  }
}
