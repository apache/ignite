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

import org.gridgain.scalar._
import scalar._
import org.gridgain.visor._
import org.gridgain.visor.commands.{VisorConsoleCommand, VisorTextTable}
import visor._
import org.gridgain.grid._
import org.gridgain.grid.kernal.GridEx
import resources._
import collection.JavaConversions._
import java.util.UUID
import scala.util.control.Breaks._
import org.jetbrains.annotations.Nullable
import util.scala.impl
import org.gridgain.grid.kernal.processors.task.GridInternal
import org.gridgain.grid.lang.GridCallable

/**
 * ==Overview==
 * Visor 'ccompact' command implementation.
 *
 * ==Help==
 * {{{
 * +--------------------------------------------------------+
 * | ccompact | Compacts all entries in cache on all nodes. |
 * +--------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     ccompact
 *     ccompact -c=<cache-name>
 * }}}
 *
 * ====Arguments====
 * {{{
 *     <cache-name>
 *         Name of the cache.
 *         If not specified, entries in default cache will be compacted.
 * }}}
 *
 * ====Examples====
 * {{{
 *     ccompact
 *         Compacts entries in default cache.
 *     ccompact -c=cache
 *         Compacts entries in cache with name 'cache'.
 * }}}
 */
class VisorCacheCompactCommand {
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
     * Compacts entries in cache.
     *
     * ===Examples===
     * <ex>cache -compact -c=cache</ex>
     * Compacts entries in cache with name 'cache'.
     *
     * @param argLst Command arguments.
     */
    def compact(argLst: ArgList, node: Option[GridNode]) = breakable {
        val cacheArg = argValue("c", argLst)

        if (cacheArg.isEmpty)
            scold("Cache name is empty.").^^

        val caches = getVariable(cacheArg.get)

        val prj = if (node.isDefined) grid.forNode(node.get) else grid.forCache(caches)

        if (prj.isEmpty) {
            val msg =
                if (caches == null)
                    "Can't find nodes with default cache."
                else
                    "Can't find nodes with specified cache: " + caches

            scold(msg).^^
        }

        val res = prj.compute()
            .withName("visor-ccompact-task")
            .withNoFailover()
            .broadcast(new CompactClosure(caches)).get

        val t = VisorTextTable()

        t #= ("Node ID8(@)", "Entries Compacted", "Cache Size Before", "Cache Size After")

        res.foreach(r => t += (nodeId8(r._1), r._2, r._3, r._4))

        t.render()
    }
}

/**
 * Compact cache entries task.
 */
@GridInternal
class CompactClosure(val cacheName: String) extends GridCallable[(UUID, Int, Int, Int)] {
    @GridInstanceResource
    private val g: Grid = null

    @impl def call(): (UUID, Int, Int, Int) = {
        val c = g.asInstanceOf[GridEx].cachex[AnyRef, AnyRef](cacheName)

        val oldSize = c.size

        val cnt = (c.keySet :\ 0)((k, cnt) => if (c.compact(k)) cnt + 1 else cnt)

        (g.localNode.id, cnt, oldSize, c.size)
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorCacheCompactCommand {
    /** Singleton command. */
    private val cmd = new VisorCacheCompactCommand

    /**
     * Singleton.
     */
    def apply() = cmd
}
