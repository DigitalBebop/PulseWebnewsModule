import sbtprotobuf.{ProtobufPlugin=>PB}
import AssemblyKeys._

name := "Pulse WebNews Module"

version := "1.0"

scalaVersion := "2.10.4"

Seq(PB.protobufSettings: _*)

version in protobufConfig := "2.6.1"

libraryDependencies += "commons-net" % "commons-net" % "3.3"

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5"

libraryDependencies += "commons-io" % "commons-io" % "2.4"

libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5"

assemblySettings

jarName in assembly := "nntpModule.jar"

mainClass in assembly := Some("net.digitalbebop.pulsetestmodule.Main")
