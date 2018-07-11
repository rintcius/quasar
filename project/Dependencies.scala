package quasar.project

import scala.Boolean
import scala.collection.Seq

import sbt._

object Dependencies {
  private val algebraVersion      = "0.7.0"
  private val argonautVersion     = "6.2"
  private val disciplineVersion   = "0.7.2"
  private val doobieVersion       = "0.4.4"
  private val jawnVersion         = "0.12.1"
  private val jacksonVersion      = "2.4.4"
  private val matryoshkaVersion   = "0.18.3"
  private val monocleVersion      = "1.4.0"
  private val pathyVersion        = "0.2.11"
  private val raptureVersion      = "2.0.0-M9"
  private val refinedVersion      = "0.8.3"
  private val scodecBitsVersion   = "1.1.2"
  private val scodecScalazVersion = "1.4.1a"
  private val http4sVersion       = "0.18.13"
  private val scalacheckVersion   = "1.13.4"
  private val scalazVersion       = "7.2.23"
  private val scalazStreamVersion = "0.8.6a"
  private val scoptVersion        = "3.5.0"
  private val shapelessVersion    = "2.3.2"
  private val simulacrumVersion   = "0.10.0"
  private val specsVersion        = "4.1.0"
  private val spireVersion        = "0.14.1"
  private val akkaVersion         = "2.5.1"
  private val fs2Version          = "1.0.0-M1"
  private val qdataVersion        = "1.0.4"

  def foundation = Seq(
    "com.slamdata"               %% "slamdata-predef"           % "0.0.4",
    "org.scalaz"                 %% "scalaz-core"               % scalazVersion,
    "org.scalaz"                 %% "scalaz-concurrent"         % scalazVersion,
    "org.scalaz.stream"          %% "scalaz-stream"             % scalazStreamVersion,
    "com.codecommit"             %% "shims"                     % "1.2.1",
    "org.typelevel"              %% "cats-effect"               % "1.0.0-RC2",
    "co.fs2"                     %% "fs2-core"                  % fs2Version,
    "co.fs2"                     %% "fs2-io"                    % fs2Version,
    "com.github.julien-truffaut" %% "monocle-core"              % monocleVersion,
    "org.typelevel"              %% "algebra"                   % algebraVersion,
    "org.typelevel"              %% "spire"                     % spireVersion,
    "io.argonaut"                %% "argonaut"                  % argonautVersion,
    "io.argonaut"                %% "argonaut-scalaz"           % argonautVersion,
    "com.slamdata"               %% "matryoshka-core"           % matryoshkaVersion,
    "com.slamdata"               %% "pathy-core"                % pathyVersion,
    "com.slamdata"               %% "pathy-argonaut"            % pathyVersion,
    "com.slamdata"               %% "qdata-time"                % qdataVersion,
    "eu.timepit"                 %% "refined"                   % refinedVersion,
    "com.chuusai"                %% "shapeless"                 % shapelessVersion,
    "org.scalacheck"             %% "scalacheck"                % scalacheckVersion,
    "com.propensive"             %% "contextual"                % "1.0.1",
    "io.frees"                   %% "iotaz-core"                % "0.3.8",
    "com.github.mpilquist"       %% "simulacrum"                % simulacrumVersion                    % Test,
    "org.typelevel"              %% "algebra-laws"              % algebraVersion                       % Test,
    "org.typelevel"              %% "discipline"                % disciplineVersion                    % Test,
    "org.typelevel"              %% "spire-laws"                % spireVersion                         % Test,
    "org.specs2"                 %% "specs2-core"               % specsVersion                         % Test,
    "org.specs2"                 %% "specs2-scalacheck"         % specsVersion                         % Test,
    "org.specs2"                 %% "specs2-scalaz"             % specsVersion                         % Test,
    "org.scalaz"                 %% "scalaz-scalacheck-binding" % (scalazVersion + "-scalacheck-1.13") % Test,
    "org.typelevel"              %% "shapeless-scalacheck"      % "0.6.1"                              % Test
  )

  def api = Seq(
    "com.github.julien-truffaut" %% "monocle-macro"      % monocleVersion,
    "eu.timepit"                 %% "refined-scalaz"     % refinedVersion,
    "eu.timepit"                 %% "refined-scalacheck" % refinedVersion % Test
  )

  def frontend = Seq(
    "com.github.julien-truffaut" %% "monocle-macro" % monocleVersion,
    "com.slamdata"               %% "qdata-core"    % qdataVersion,
    "com.slamdata"               %% "qdata-core"    % qdataVersion % "test->test" classifier "tests",
    "com.slamdata"               %% "qdata-time"    % qdataVersion % "test->test" classifier "tests",
    "org.typelevel"              %% "algebra-laws"  % algebraVersion % Test
  )

  def effect = Seq(
    "com.fasterxml.uuid" % "java-uuid-generator" % "3.1.4"
  )

  def datagen = Seq(
    "com.github.scopt" %% "scopt"          % scoptVersion,
    "eu.timepit"       %% "refined-scalaz" % refinedVersion
  )

  def sql = Seq(
    "com.github.julien-truffaut" %% "monocle-macro" % monocleVersion,
    "org.scala-lang.modules"     %% "scala-parser-combinators" % "1.0.6"
  )

  def core = Seq(
    "org.tpolecat"               %% "doobie-core"               % doobieVersion,
    "org.tpolecat"               %% "doobie-hikari"             % doobieVersion,
    "org.tpolecat"               %% "doobie-postgres"           % doobieVersion,
    "org.http4s"                 %% "http4s-core"               % http4sVersion,
    "com.github.julien-truffaut" %% "monocle-macro"             % monocleVersion,
    "com.github.tototoshi"       %% "scala-csv"                 % "1.3.4",
    "com.slamdata"               %% "pathy-argonaut"            % pathyVersion,
    // Removing this will not cause any compile time errors, but will cause a runtime error once
    // Quasar attempts to connect to an h2 database to use as a metastore
    "com.h2database"              % "h2"                        % "1.4.196",
    "org.tpolecat"               %% "doobie-specs2"             % doobieVersion % Test
  )

  def impl = Seq(
    "org.http4s"     %% "jawn-fs2"      % "0.13.0-M1",
    "org.spire-math" %% "jawn-argonaut" % jawnVersion
  )

  def interface = Seq(
    "com.github.scopt" %% "scopt" % scoptVersion,
    "org.jboss.aesh"    % "aesh"  % "0.66.17",
    "org.jline" % "jline" % "3.8.0"
  )

  def mongodb = {
    val nettyVersion = "4.1.21.Final"

    Seq(
      "org.mongodb" % "mongodb-driver-async" %   "3.6.3",
      // These are optional dependencies of the mongo asynchronous driver.
      // They are needed to connect to mongodb vis SSL which we do under certain configurations
      "io.netty"    % "netty-buffer"         % nettyVersion,
      "io.netty"    % "netty-handler"        % nettyVersion
    )
  }

  def precog = Seq(
    "org.slf4s"            %% "slf4s-api"       % "1.7.25",
    "org.slf4j"            %  "slf4j-log4j12"   % "1.7.16",
    "org.typelevel"        %% "spire"           % spireVersion,
    "org.scodec"           %% "scodec-scalaz"   % scodecScalazVersion,
    "org.scodec"           %% "scodec-bits"     % scodecBitsVersion,
    "org.apache.jdbm"      %  "jdbm"            % "3.0-alpha5",
    "com.typesafe.akka"    %%  "akka-actor"     % akkaVersion,
    ("org.quartz-scheduler" %  "quartz"         % "2.3.0")
      .exclude("com.zaxxer", "HikariCP-java6"), // conflict with Doobie
    "commons-io"           %  "commons-io"      % "2.5"
  )

  def blueeyes = Seq(
    "com.google.guava" % "guava" % "13.0"
  )

  def yggdrasil = Seq(
    "com.codecommit" %% "smock" % "0.4.0-specs2-4.0.2" % "test"
  )

  def niflheim = Seq(
    "com.typesafe.akka"  %% "akka-actor" % akkaVersion,
    "org.typelevel"      %% "spire"      % spireVersion,
    "org.objectweb.howl" %  "howl"       % "1.0.1-1"
  )

  def it = Seq(
    "co.fs2"           %% "fs2-io"              % fs2Version          % Test,
    "io.argonaut"      %% "argonaut-monocle"    % argonautVersion     % Test,
    "eu.timepit"       %% "refined-scalacheck"  % refinedVersion      % Test,
    "io.verizon.knobs" %% "core"                % "4.0.30-scalaz-7.2" % Test
  )
}
