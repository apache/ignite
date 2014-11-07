/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.cache

import org.gridgain.grid.cache.query.GridCacheQuerySqlField
import org.gridgain.grid.{GridGain => G}
import org.gridgain.visor._
import org.gridgain.visor.commands.cache.VisorCacheCommand._
import org.scalatest._

/**
 * Unit test for 'events' command.
 */
class VisorCacheCommandSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
    /**
     * Open Visor.
     */
    override def beforeAll() {
        val g = G.start("examples/config/example-cache.xml")

        assert(g.caches().size() > 0)

        visor.open("-e")
    }

    /**
     * Close Visor.
     */
    override def afterAll() {
        visor.close()

        G.stop(false)
    }

    behavior of "A 'cache' visor command"

    it should "put/get some values to/from cache and display information about caches" in {
        val c = G.grid.cache[String, String]("partitioned")

        for (i <- 0 to 3) {
            val kv = "" + i

            c.put(kv, kv)

            c.get(kv)
        }

        visor.cache()
    }

    it should "run query and display information about caches" in {
        val g = G.grid

        val c = g.cache[Int, Foo]("replicated")

        c.put(0, Foo(20))
        c.put(1, Foo(100))
        c.put(2, Foo(101))
        c.put(3, Foo(150))

        // Create two queries
        val q1 = c.queries().createSqlQuery(classOf[Foo], "_key > ?")
        c.queries().createSqlQuery(classOf[Foo], "_key = ?")

        // Execute only one query
        q1.execute(100.asInstanceOf[java.lang.Integer]).get

        visor cache "-a"
    }

    it should "display correct information for 'replicated' cache only" in {
        visor cache "-n=replicated -a"
    }

    it should "display correct information for all caches" in {
        visor cache "-a"
    }
}

/**
 * Object for queries.
 */
private case class Foo(
    @GridCacheQuerySqlField
    value: Int
)
