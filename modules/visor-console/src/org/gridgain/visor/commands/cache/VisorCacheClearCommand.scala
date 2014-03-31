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

import java.util.UUID
import org.gridgain.grid.kernal.GridEx
import org.gridgain.grid.kernal.processors.task.GridInternal
import org.gridgain.grid.lang.GridCallable
import org.gridgain.grid.resources._
import org.gridgain.grid.util.scala.impl
import org.gridgain.scalar._
import scalar._
import org.gridgain.visor._
import visor._
import org.gridgain.visor.commands.VisorTextTable
import scala.collection.JavaConversions._
import scala.util.control.Breaks._
import org.gridgain.grid.GridNode

/**
 * ==Overview==
 * Visor 'clear' command implementation.
 *
 * ==Help==
 * {{{
 * +------------------------------------------------------------+
 * | cache -clear | Clears all entries from cache on all nodes. |
 * +------------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     cache -clear
 *     cache -clear -c=<cache-name>
 * }}}
 *
 * ====Arguments====
 * {{{
 *     <cache-name>
 *         Name of the cache.
 *         If not specified, default cache will be cleared.
 * }}}
 *
 * ====Examples====
 * {{{
 *     cache -clear
 *         Clears default cache.
 *     cache -clear -c=cache
 *         Clears cache with name 'cache'.
 * }}}
 */
class VisorCacheClearCommand {
    /**
     * Prints error message and advise.
     *
     * @param errMsgs Error messages.
     */
    private def scold(errMsgs: Any*) {
        assert(errMsgs != null)

        warn(errMsgs: _*)
        warn("Type 'help cache' to see how to use this command.")
    }

    /**
     * ===Command===
     * Clears cache by its name.
     *
     * ===Examples===
     * <ex>cache -clear -c=cache</ex>
     * Clears cache with name 'cache'.
     *
     * @param argLst Command arguments.
     */
    def clear(argLst: ArgList, node: Option[GridNode]) = breakable {
        val cacheArg = argValue("c", argLst)

        val cache = if (cacheArg.isEmpty) null else cacheArg.get

        val prj = if (node.isDefined) grid.forNode(node.get) else grid.forCache(cache)

        if (prj.isEmpty) {
            val msg =
                if (cache == null)
                    "Can't find nodes with default cache."
                else
                    "Can't find nodes with specified cache: " + cache

            scold(msg).^^
        }

        val res = prj
            .compute()
            .withName("visor-cclear-task")
            .withNoFailover()
            .broadcast(new ClearClosure(cache))
            .get

        println("Cleared cache with name: " + (if (cache == null) "<default>" else cache))

        val t = VisorTextTable()

        t #= ("Node ID8(@)", "Cache Size Before", "Cache Size After")

        res.foreach(r => t += (nodeId8(r._1), r._2, r._3))

        t.render()
    }
}

/**
 * Clear cache task.
 */
@GridInternal
class ClearClosure(val cacheName: String) extends GridCallable[(UUID, Int, Int)] {
    @GridInstanceResource
    private val g: GridEx = null

    @impl def call(): (UUID, Int, Int) = {
        val c = g.cachex[AnyRef, AnyRef](cacheName)

        val oldSize = c.size

        c.clearAll()

        (g.localNode.id, oldSize, c.size)
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorCacheClearCommand {
    /** Singleton command. */
    private val cmd = new VisorCacheClearCommand

    /**
     * Singleton.
     */
    def apply() = cmd
}
