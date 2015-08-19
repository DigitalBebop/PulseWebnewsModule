package net.digitalbebop.pulsetestmodule


import java.io.Reader
import java.security.SecureRandom
import java.security.cert.X509Certificate
import javax.net.ssl._

import com.google.protobuf.ByteString
import net.digitalbebop.ClientRequests.IndexRequest
import org.apache.commons.io.IOUtils
import org.apache.commons.net.nntp.{NewsgroupInfo, NNTPClient}
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ByteArrayEntity
import org.apache.http.impl.client.HttpClients

import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
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

  val hostname = "news.csh.rit.edu"
  val port = 563
  val moduleName = "nntp"
  val apiAddress = "http://localhost:8080"


  lazy val getSocketFactory: SSLSocketFactory = {
    val tm: Array[TrustManager] = Array(new NaiveTrustManager())
    val context = SSLContext.getInstance("SSL")
    context.init(Array[KeyManager](), tm, new SecureRandom())
    context.getSocketFactory
  }

  def createMessage(indexData: String, channel: String, id: Long, headers: Map[String, String]): IndexRequest = {
    val builder = IndexRequest.newBuilder()
    builder.setIndexData(indexData)

    val json = new JSONObject(Map(("format", "text"), ("date", headers("Date")), ("channel", channel)))
    builder.setMetaTags(json.toString())

    builder.setModuleId(channel + "-" + id.toString)
    builder.setModuleName(moduleName)
    builder.setRawData(ByteString.copyFromUtf8(indexData))
    builder.addTags("news")
    builder.addTags(channel.split('.').last)
    headers.get("From") match {
      case Some(str) =>
        val splits = str.replace('<', ' ').replace('>', ' ').replace('"', ' ')
          .split(" ").filter(_ != "").map(_.toLowerCase)
        splits.foreach(builder.addTags)
        val email = splits.last
        builder.setUsername(email.split("@").head)
      case None =>
    }
    builder.build()
  }

  def parseHeaders(reader: Reader): Map[String, String] = {
    val input = IOUtils.toString(reader)
    input.split("\n").map { line =>
      val splits = line.split(":", 2).take(2).map(_.trim)
      (splits(0), splits(1))
    }.toMap
  }

  def processGroup(group: NewsgroupInfo): Unit = try {
    val client = new NNTPClient()
    client.setSocketFactory(getSocketFactory)
    client.connect(hostname, port)
    println(client.authenticate(username, password))

    println(s"group = ${group.getNewsgroup}")
    client.selectNewsgroup(group.getNewsgroup)
    val first = group.getFirstArticleLong
    val last = group.getLastArticleLong

    (first to last).foreach { index =>
      try {
        println(s"index = $index")
        val headers = parseHeaders(client.retrieveArticleHeader(index))
        val body = IOUtils.toString(client.retrieveArticleBody(index))
        val message = createMessage(body, group.getNewsgroup, index, headers)
        val post = new HttpPost(s"$apiAddress/api/index")
        post.setEntity(new ByteArrayEntity(message.toByteArray))
        HttpClients.createDefault().execute(post)
      } catch {
        case ex: Exception => println(s"Error processing: ${group.getNewsgroup} - $index")
      }
    }
  } catch {
    case ex: Exception => println(s"error processing group ${group.getNewsgroup}: ${ex.getMessage}")
      throw ex
  }

  def main(args: Array[String]): Unit = {

    val address = if (args.length > 0) args(0) else "http://localhost:8080"
    println(s"using address: $address, specify in arguments to override")

    print("username: ")
    username = readLine()
    print("password: ")
    password = System.console().readPassword().mkString("")
    println(password)

    val client = new NNTPClient()
    client.setSocketFactory(getSocketFactory)
    client.connect(hostname, port)
    println(client.authenticate(username, password))

    val futures = client.listNewsgroups().map { group =>
      Future { processGroup(group) }
    }

    Await.ready(Future.sequence(futures.toList), Duration.Inf)
  }
}
