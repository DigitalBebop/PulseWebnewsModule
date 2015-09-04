import sbtprotobuf.{ProtobufPlugin=>PB}
import AssemblyKeys._

name := "Pulse WebNews Module"

version := "1.0"

scalaVersion := "2.10.4"

Seq(PB.protobufSettings: _*)

version in protobufConfig := "2.6.1"

libraryDependencies ++= Seq("commons-net" % "commons-net" % "3.3",
  "org.apache.httpcomponents" % "httpclient" % "4.5",
  "commons-io" % "commons-io" % "2.4",
  "org.apache.httpcomponents" % "httpclient" % "4.5",
  "commons-cli" % "commons-cli" % "1.3.1",
  "com.google.code.gson" % "gson" % "2.3.1")

    assemblySettings

jarName in assembly := "nntpModule.jar"

mainClass in assembly := Some("net.digitalbebop.pulsewebnewsmodule.Main")
