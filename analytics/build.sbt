name := "control-tower-analytics"

version := "0.1"

scalaVersion := "2.12.15"

val awsSdkVersion      = "1.12.429"
val apacheFlinkVersion = "1.17.2"
val apacheHttpVersion  = "4.5.14"

libraryDependencies ++= Seq(
  "com.amazonaws"             % "aws-java-sdk-kinesis"       % awsSdkVersion,
  "org.apache.flink"          % "flink-clients"              % apacheFlinkVersion,
  "org.apache.flink"          % "flink-connector-kinesis"    % "4.0.0-1.16",
  "org.apache.flink"          % "flink-statebackend-rocksdb" % apacheFlinkVersion,
  "org.apache.flink"          % "flink-s3-fs-hadoop"         % apacheFlinkVersion,
  "org.apache.flink"          %% "flink-streaming-scala"     % apacheFlinkVersion,
  "org.apache.httpcomponents" % "httpclient"                 % apacheHttpVersion,
  "ch.qos.logback"            % "logback-classic"            % "1.4.12",
  "com.thesamet.scalapb"      %% "scalapb-runtime"           % scalapb.compiler.Version.scalapbVersion % "protobuf"
)

Compile / PB.targets := Seq(
  scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"
)

Compile / PB.protoSources := Seq(file("../protobuf"))
