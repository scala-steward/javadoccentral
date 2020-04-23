import java.io.File
import java.nio.file.Files

import cats.effect.{Blocker, ExitCode, IO, IOApp}
import cats.implicits._
import org.http4s.client.Client
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.middleware.Logger
import org.http4s.dsl.impl.Root
import org.http4s.dsl.io._
import org.http4s.headers.Location
import org.http4s.implicits._
import org.http4s.server.blaze.BlazeServerBuilder
import org.http4s.twirl._
import org.http4s.{HttpRoutes, Request, Response, StaticFile, Uri}

import scala.concurrent.ExecutionContext._

object App extends IOApp {

  def needGroupId(maybeGroupId: Option[String]): IO[Response[IO]] = {
    maybeGroupId.fold(Ok(html.needGroupId())) { groupId =>
      PermanentRedirect(Location(Uri.unsafeFromString(s"/$groupId")))
    }
  }


  def needArtifactId(groupId: String, maybeArtifactId: Option[String])(implicit client: Client[IO]): IO[Response[IO]] = {
    maybeArtifactId.fold {
      // todo: not found & bad request for other errors
      MavenCentral.searchArtifacts(groupId).flatMap { artifactIds =>
        Ok(html.needArtifactId(groupId, artifactIds))
      }
    } { artifactId =>
      PermanentRedirect(Location(Uri.unsafeFromString(s"/$groupId/$artifactId")))
    }
  }

  def needVersion(groupId: String, artifactId: String, maybeVersion: Option[String])(implicit client: Client[IO]) = {
    maybeVersion.fold {
      // todo: not found & bad request for other errors
      MavenCentral.searchVersions(groupId, artifactId)(client).flatMap { versions =>
        // todo: version sorting
        Ok(html.needVersion(groupId, artifactId, versions))
      }
    } { version =>
      PermanentRedirect(Location(Uri.unsafeFromString(s"/$groupId/$artifactId/$version")))
    }
  }

  def index(groupId: String, artifactId: String, version: String)(implicit client: Client[IO]) = {
    if (version == "latest") {
      MavenCentral.latest(groupId, artifactId).flatMap { maybeLatestVersion =>
        val uri = maybeLatestVersion.fold(Uri.unsafeFromString(s"/$groupId/$artifactId")) { latestVersion =>
          Uri.unsafeFromString(s"/$groupId/$artifactId/$latestVersion")
        }
        TemporaryRedirect(Location(uri))
      }
    }
    else {
      MavenCentral.artifactExists(groupId, artifactId, version).flatMap { exists =>
        if (exists) {
          PermanentRedirect(Location(Uri.unsafeFromString(s"/$groupId/$artifactId/$version/index.html")))
        }
        else {
          NotFound("The specified Maven Central module does not exist.")
        }
      }
    }
  }

  def file(groupId: String, artifactId: String, version: String, filepath: Path, request: Request[IO])(implicit client: Client[IO], tmpDir: File, blocker: Blocker) = {
    val javadocUri = MavenCentral.javadocUri(groupId, artifactId, version)
    val javadocDir = new File(tmpDir, s"$groupId/$artifactId/$version")
    val javadocFile = new File(javadocDir, filepath.toString)

    println("maybe dowloading")
    // todo: fix race condition
    val extracted = if (!javadocDir.exists()) {
      println("dowloading")
      MavenCentral.downloadAndExtractZip(javadocUri, javadocDir).flatMap { _ =>
        println("downloaded")
        IO.unit
      }
    }
    else {
      IO.unit
    }

    extracted.flatMap { _ =>
      if (javadocFile.exists()) {
        println("serving")
        StaticFile.fromFile(javadocFile, blocker, Some(request)).getOrElseF(NotFound())
      }
      else {
        NotFound("The specified file does not exist.")
      }
    }
  }

  object OptionalGroupIdQueryParamMatcher extends OptionalQueryParamDecoderMatcher[String]("groupId")
  object OptionalArtifactIdQueryParamMatcher extends OptionalQueryParamDecoderMatcher[String]("artifactId")
  object OptionalVersionQueryParamMatcher extends OptionalQueryParamDecoderMatcher[String]("version")

  def httpApp(implicit client: Client[IO], tmpDir: File, blocker: Blocker) = HttpRoutes.of[IO] {
    case GET -> Root :? OptionalGroupIdQueryParamMatcher(maybeGroupId) => needGroupId(maybeGroupId)
    case GET -> Root / groupId :? OptionalArtifactIdQueryParamMatcher(maybeArtifactId) => needArtifactId(groupId, maybeArtifactId)
    case GET -> Root / groupId / artifactId :? OptionalVersionQueryParamMatcher(maybeVersion) => needVersion(groupId, artifactId, maybeVersion)
    case GET -> Root / groupId / artifactId / version => index(groupId, artifactId, version)
    case GET -> Root / groupId / artifactId / version / "" => index(groupId, artifactId, version)
    case req @ GET -> groupId /: artifactId /: version /: filepath => file(groupId, artifactId, version, filepath, req)
  }.orNotFound

  //val finalHttpApp = Logger.httpApp(true, true)(httpApp)


  def run(args: List[String]) = {
    val port = sys.env.getOrElse("PORT", "8080").toInt

    val tmpDir = Files.createTempDirectory("jars").toFile // todo: to resource

    {
      for {
        blocker <- Blocker[IO]
        client <- BlazeClientBuilder[IO](global).resource
        //loggerClient = Logger(true, true)(client)
        httpAppWithClient = httpApp(client, tmpDir, blocker)
        server <- BlazeServerBuilder[IO].bindHttp(port, "0.0.0.0").withHttpApp(httpAppWithClient).resource
      } yield server
    }.use(_ => IO.never).as(ExitCode.Success)
  }

}
