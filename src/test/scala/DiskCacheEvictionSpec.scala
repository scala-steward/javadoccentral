import com.jamesward.zio_mavencentral.MavenCentral
import zio.*
import zio.cache.{Cache, Lookup}
import zio.direct.*
import zio.test.*

import java.io.File
import java.nio.file.Files

/**
 * Tests for the time-based disk-cache eviction introduced alongside the
 * 2h TTL on JavadocCache/SourcesCache. The mechanism is:
 *
 * 1. `DiskCacheCoordinator` tracks last-access time + per-GAV semaphores.
 * 2. `Extractor.evictGav` invalidates the cache entry, sleeps a grace
 *    period to let in-flight readers finish, then (under the per-GAV
 *    semaphore) deletes the directory and forgets the GAV.
 * 3. A fetch that runs concurrently with eviction synchronizes via the
 *    same per-GAV semaphore, so it never races with the delete.
 */
object DiskCacheEvictionSpec extends ZIOSpecDefault:

  private val gav = Extractor.gav("test.group", "test-artifact", "1.0.0")

  // Makes a temporary directory + a File pointing to a per-GAV subdir inside it.
  // The subdir is pre-populated with one file, mimicking the state after a
  // successful javadoc extraction.
  private def mkTmpDirWithGavContent: ZIO[Scope, Nothing, (File, File)] =
    defer:
      val tmp = ZIO.attempt(Files.createTempDirectory("eviction-test").nn.toFile).orDie.run
      ZIO.addFinalizer(ZIO.attempt(recursivelyDelete(tmp)).ignore).run
      val gavDir = File(tmp, gav.toString)
      ZIO.attempt:
        gavDir.mkdirs()
        val marker = File(gavDir, "marker.txt")
        Files.writeString(marker.toPath, "hello")
        ()
      .orDie.run
      (tmp, gavDir)

  private def recursivelyDelete(f: File): Unit =
    if f.isDirectory then
      val cs = f.listFiles
      if cs != null then
        var i = 0
        while i < cs.length do
          val c = cs(i)
          if c != null then recursivelyDelete(c)
          i += 1
    f.delete()
    ()

  // Builds a Cache that returns the given File and never actually fetches
  // anything. Sufficient for exercising evictGav's cache.invalidate path.
  private def fixedCache(returning: File): UIO[Cache[MavenCentral.GroupArtifactVersion, MavenCentral.NotFoundError, File]] =
    Cache.make(
      capacity = 10,
      timeToLive = Duration.Infinity,
      lookup = Lookup[MavenCentral.GroupArtifactVersion, Any, MavenCentral.NotFoundError, File](_ => ZIO.succeed(returning)),
    )

  def spec = suite("DiskCache eviction")(

    test("evictGav invalidates the cache entry and deletes the directory") {
      defer:
        val (_, gavDir) = mkTmpDirWithGavContent.run
        val coordinator = Extractor.DiskCacheCoordinator.make.run
        val cache = fixedCache(gavDir).run

        // Populate cache + tracker
        cache.get(gav).run
        coordinator.touch(gav).run

        val existedBefore = gavDir.exists()
        val cachedBefore = cache.contains(gav).run
        val trackedBefore = coordinator.lastAccess.get(gav).run.isDefined

        Extractor.evictGav(cache, coordinator, gav, gavDir, gracePeriod = Duration.Zero).run

        val existedAfter = gavDir.exists()
        val cachedAfter = cache.contains(gav).run
        val trackedAfter = coordinator.lastAccess.get(gav).run.isDefined

        assertTrue(
          existedBefore,
          cachedBefore,
          trackedBefore,
          !existedAfter,
          !cachedAfter,
          !trackedAfter,
        )
    },

    test("evictGav waits out the grace period before deleting") {
      // During the grace period the cache entry is already invalidated
      // but the directory still exists so in-flight readers can finish.
      defer:
        val (_, gavDir) = mkTmpDirWithGavContent.run
        val coordinator = Extractor.DiskCacheCoordinator.make.run
        val cache = fixedCache(gavDir).run

        cache.get(gav).run
        coordinator.touch(gav).run

        val evictFiber = Extractor.evictGav(cache, coordinator, gav, gavDir, gracePeriod = 5.seconds).fork.run

        // Let the invalidate happen, but stay inside the grace period
        TestClock.adjust(100.millis).run
        // Cache should be invalidated already, directory still present.
        val midCached = cache.contains(gav).run
        val midDirExists = gavDir.exists()

        // Advance past the grace period
        TestClock.adjust(6.seconds).run
        evictFiber.join.run

        assertTrue(
          !midCached,     // cache invalidated immediately
          midDirExists,   // directory still there during grace
          !gavDir.exists(), // but gone after grace
        )
    },

    test("staleGavs returns only GAVs whose last-access is older than maxIdle") {
      defer:
        val coordinator = Extractor.DiskCacheCoordinator.make.run
        val fresh = Extractor.gav("g", "fresh", "1")
        val stale = Extractor.gav("g", "stale", "1")

        coordinator.touch(stale).run
        // Pretend two hours passed
        TestClock.adjust(2.hours + 1.minute).run
        coordinator.touch(fresh).run

        val result = Extractor.staleGavs(coordinator, 2.hours).run

        assertTrue(
          result.contains(stale),
          !result.contains(fresh),
        )
    },

    test("eviction waits for an in-progress fetch (semaphore serialization)") {
      // Simulates: fetch grabs the semaphore and holds it across a "download".
      // Eviction fires. The delete half of eviction must wait until fetch
      // releases the semaphore.
      defer:
        val (_, gavDir) = mkTmpDirWithGavContent.run
        val coordinator = Extractor.DiskCacheCoordinator.make.run
        val cache = fixedCache(gavDir).run

        cache.get(gav).run
        coordinator.touch(gav).run

        val sem = coordinator.lockFor(gav).run
        val fetchStarted = Promise.make[Nothing, Unit].run
        val fetchRelease = Promise.make[Nothing, Unit].run

        // Simulated fetch holding the lock
        val fetchFiber =
          sem.withPermit(fetchStarted.succeed(()) *> fetchRelease.await).fork.run

        fetchStarted.await.run

        // Start eviction while fetch holds the lock
        val evictFiber =
          Extractor.evictGav(cache, coordinator, gav, gavDir, gracePeriod = Duration.Zero).fork.run

        // Give the eviction fiber a chance to try to acquire the permit.
        // The cache.invalidate step runs *before* the permit is acquired, so
        // it should already be done. The delete, however, should be blocked.
        TestClock.adjust(200.millis).run
        val cachedWhileFetching = cache.contains(gav).run
        val dirExistsWhileFetching = gavDir.exists()

        // Let the fetch complete so the eviction can proceed
        fetchRelease.succeed(()).run
        fetchFiber.join.run
        evictFiber.join.run

        assertTrue(
          !cachedWhileFetching,     // invalidate ran before blocking on lock
          dirExistsWhileFetching,   // but delete was blocked
          !gavDir.exists(),         // and proceeded once fetch released
        )
    },

  ) @@ TestAspect.withLiveRandom
