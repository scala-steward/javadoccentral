import com.jamesward.zio_mavencentral.MavenCentral
import zio.*
import zio.cache.{Cache, Lookup}
import zio.concurrent.ConcurrentMap
import zio.direct.*
import zio.http.*
import zio.redis.{CodecSupplier, Redis, RedisConfig, RedisError}

import java.net.URI
import java.nio.file.Files

object App extends ZIOAppDefault:

  val server =
    ZLayer.fromZIO:
      defer:
        val system = ZIO.system.run
        val maybePort = system.env("PORT").run.flatMap(_.toIntOption)
        maybePort.fold(Server.default)(Server.defaultWithPort)
    .flatten

  val blockerLayer: ZLayer[Any, Nothing, Extractor.FetchBlocker] =
    ZLayer.fromZIO(ConcurrentMap.empty[MavenCentral.GroupArtifactVersion, Promise[Nothing, Unit]].map(Extractor.FetchBlocker(_)))

  val sourcesBlockerLayer: ZLayer[Any, Nothing, Extractor.FetchSourcesBlocker] =
    ZLayer.fromZIO(ConcurrentMap.empty[MavenCentral.GroupArtifactVersion, Promise[Nothing, Unit]].map(Extractor.FetchSourcesBlocker(_)))

  val latestCacheLayer: ZLayer[Client & Scope, Nothing, Extractor.LatestCache] = ZLayer.fromZIO:
    Cache.makeWith(1_000, Lookup(Extractor.latest)):
      case Exit.Success(_) => 1.hour
      case Exit.Failure(_) => Duration.Zero
    .map(Extractor.LatestCache(_))

  // Directories extracted for cached javadoc/sources entries live on the
  // dyno's ephemeral filesystem and contribute to memory pressure via the
  // kernel page cache. A 2-hour TTL (with a background janitor that deletes
  // the directory when a cache entry expires) keeps disk usage bounded for
  // the common traffic pattern: a burst of requests for one artifact, then
  // quiet. Capacity is also dropped to 100 entries to bound worst-case disk
  // usage within any 2-hour window.
  val javadocCacheTtl: Duration = 2.hours
  val javadocCacheCapacity: Int = 100
  val javadocEvictGracePeriod: Duration = 60.seconds

  val javadocDiskCoordinatorLayer: ZLayer[Any, Nothing, Extractor.JavadocDiskCoordinator] =
    ZLayer.fromZIO(Extractor.DiskCacheCoordinator.make.map(Extractor.JavadocDiskCoordinator(_)))

  val sourcesDiskCoordinatorLayer: ZLayer[Any, Nothing, Extractor.SourcesDiskCoordinator] =
    ZLayer.fromZIO(Extractor.DiskCacheCoordinator.make.map(Extractor.SourcesDiskCoordinator(_)))

  val javadocCacheLayer: ZLayer[Client & Extractor.FetchBlocker & Extractor.TmpDir & Extractor.JavadocDiskCoordinator & Scope, Nothing, Extractor.JavadocCache] = ZLayer.fromZIO:
    defer:
      val coordinator = ZIO.service[Extractor.JavadocDiskCoordinator].run.value
      val cache = Cache.makeWith(javadocCacheCapacity, Lookup(Extractor.javadoc)):
        case Exit.Success(_) => javadocCacheTtl
        case Exit.Failure(_) => Duration.Zero
      .run
      Extractor.JavadocCache(cache, coordinator)

  val sourcesCacheLayer: ZLayer[Client & Extractor.FetchSourcesBlocker & Extractor.TmpDir & Extractor.SourcesDiskCoordinator & Scope, Nothing, Extractor.SourcesCache] = ZLayer.fromZIO:
    defer:
      val coordinator = ZIO.service[Extractor.SourcesDiskCoordinator].run.value
      val cache = Cache.makeWith(javadocCacheCapacity, Lookup(Extractor.sources)):
        case Exit.Success(_) => javadocCacheTtl
        case Exit.Failure(_) => Duration.Zero
      .run
      Extractor.SourcesCache(cache, coordinator)

  val tmpDirLayer = ZLayer.succeed(Extractor.TmpDir(Files.createTempDirectory("jars").nn.toFile))

  val symbolSearchGuardLayer: ZLayer[Any, Nothing, SymbolSearch.SymbolSearchGuard] =
    ZLayer.fromZIO:
      ConcurrentMap.empty[MavenCentral.GroupArtifactVersion, Unit].map(SymbolSearch.SymbolSearchGuard(_))

  val redisUri: ZIO[Any, Throwable, URI] =
    ZIO.systemWith:
      system =>
        system.env("REDIS_URL")
          .someOrFail(new RuntimeException("REDIS_URL env var not set"))
          .map:
            redisUrl =>
              URI(redisUrl)

  val redisConfigLayer: ZLayer[Any, Throwable, RedisConfig] =
    ZLayer.fromZIO:
      defer:
        val uri = redisUri.run
        RedisConfig(uri.getHost, uri.getPort, ssl = true, verifyCertificate = false)

  // may not work with reconnects
  val redisAuthLayer: ZLayer[CodecSupplier & RedisConfig, Throwable, Redis] =
    Redis.singleNode.flatMap:
      env =>
        ZLayer.fromZIO:
          defer:
            val uri = redisUri.run
            val redis = env.get[Redis]
            val password = uri.getUserInfo.drop(1) // REDIS_URL has an empty username

            val authIfNeeded =
              redis.ping().catchAll:
                case e: RedisError if e.getMessage.contains("NOAUTH") =>
                  ZIO.logInfo("Redis NOAUTH detected, authenticating...") *> redis.auth(password)
                case e =>
                  ZIO.fail(e)

            redis.auth(password).run

            authIfNeeded.repeat(Schedule.spaced(5.seconds)).forkDaemon.run

            redis

  private def dirSize(dir: java.io.File): Long =
    val path = dir.toPath.nn
    if !Files.exists(path) then 0L
    else
      import java.nio.file.{Files as NioFiles, LinkOption}
      import java.nio.file.attribute.BasicFileAttributes
      var total = 0L
      val stream = NioFiles.walk(path).nn
      try
        stream.iterator.nn.forEachRemaining: p =>
          val attrs = NioFiles.readAttributes(p, classOf[BasicFileAttributes], LinkOption.NOFOLLOW_LINKS).nn
          if attrs.isRegularFile then total += attrs.size()
      finally stream.close()
      total

  private def jvmMemStats: String =
    val mxBean = java.lang.management.ManagementFactory.getMemoryMXBean
    val heap = mxBean.getHeapMemoryUsage
    val nonHeap = mxBean.getNonHeapMemoryUsage
    val threadCount = java.lang.management.ManagementFactory.getThreadMXBean.getThreadCount
    val mb = (b: Long) => b / 1024 / 1024
    s"heap_used=${mb(heap.getUsed)}MB heap_committed=${mb(heap.getCommitted)}MB nonheap_used=${mb(nonHeap.getUsed)}MB nonheap_committed=${mb(nonHeap.getCommitted)}MB threads=$threadCount"

  private val logCacheStats: ZIO[Extractor.JavadocCache & Extractor.SourcesCache & Extractor.TmpDir, Nothing, Unit] =
    defer:
      val javadocCacheSize = ZIO.serviceWithZIO[Extractor.JavadocCache](_.cache.size).run
      val sourcesCacheSize = ZIO.serviceWithZIO[Extractor.SourcesCache](_.cache.size).run
      val tmpDir = ZIO.service[Extractor.TmpDir].run.dir
      val diskBytes = ZIO.attemptBlockingIO(dirSize(tmpDir)).orDie.run
      ZIO.logInfo(s"cache stats: javadoc=$javadocCacheSize sources=$sourcesCacheSize disk=${diskBytes / 1024 / 1024}MB $jvmMemStats").run

  // How often the janitor scans for GAVs with no recent access. Shorter than
  // the TTL so that stale entries get cleaned up promptly after the idle
  // window closes.
  private val janitorInterval: Duration = 5.minutes

  // Evicts idle javadoc and sources entries whose last-access is older than
  // the cache TTL. Runs each candidate eviction concurrently via forkDaemon
  // so the semaphore inside `evictGav` doesn't block the scan itself.
  private val janitorSweep: ZIO[Extractor.JavadocCache & Extractor.SourcesCache & Extractor.TmpDir, Nothing, Unit] =
    defer:
      val tmpDir = ZIO.service[Extractor.TmpDir].run.dir
      val javadocCache = ZIO.service[Extractor.JavadocCache].run
      val sourcesCache = ZIO.service[Extractor.SourcesCache].run
      val staleJavadocs = Extractor.staleGavs(javadocCache.coordinator, javadocCacheTtl).run
      val staleSources = Extractor.staleGavs(sourcesCache.coordinator, javadocCacheTtl).run
      ZIO.foreachDiscard(staleJavadocs): gav =>
        val dir = new java.io.File(tmpDir, gav.toString)
        ZIO.logInfo(s"Janitor scheduling javadoc eviction: $gav") *>
          Extractor.evictGav(javadocCache.cache, javadocCache.coordinator, gav, dir, javadocEvictGracePeriod).forkDaemon
      .run
      ZIO.foreachDiscard(staleSources): gav =>
        val dir = new java.io.File(tmpDir, s"$gav-sources")
        ZIO.logInfo(s"Janitor scheduling sources eviction: $gav") *>
          Extractor.evictGav(sourcesCache.cache, sourcesCache.coordinator, gav, dir, javadocEvictGracePeriod).forkDaemon
      .run

  // Use virtual threads for blocking operations. On JDK 21+, virtual threads
  // are lightweight (tiny stacks, mounted on a small carrier pool) and unmount
  // cleanly during blocking I/O, so we avoid the pthread_create EAGAIN risk of
  // the default ZIO cached thread pool without artificially capping concurrency.
  // Must be applied via `bootstrap` so fibers forked by the server inherit the
  // FiberRef.
  override val bootstrap =
    Runtime.enableLoomBasedBlockingExecutor

  def run =
    // todo: log filtering so they don't show up in tests / runtime config
    val background =
      logCacheStats.repeat(Schedule.spaced(1.minute)).forkDaemon *>
        janitorSweep.repeat(Schedule.spaced(janitorInterval)).forkDaemon
    (background *> Server.serve(Web.appWithMiddleware)).provide(
      server,
      Client.default,
      Scope.default,
      blockerLayer,
      sourcesBlockerLayer,
      latestCacheLayer,
      javadocDiskCoordinatorLayer,
      sourcesDiskCoordinatorLayer,
      javadocCacheLayer,
      sourcesCacheLayer,
      tmpDirLayer,
      redisConfigLayer,
      redisAuthLayer,
      ZLayer.succeed[CodecSupplier](SymbolSearch.ProtobufCodecSupplier),
      SymbolSearch.herokuInferenceLayer,
      BadActor.live,
      Web.crawlerGavLimiterLayer,
      symbolSearchGuardLayer,
    )
