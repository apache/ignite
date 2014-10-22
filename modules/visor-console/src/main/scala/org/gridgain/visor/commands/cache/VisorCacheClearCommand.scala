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

import java.util.Collections

import org.gridgain.grid.GridNode
import org.gridgain.grid.kernal.visor.cmd.VisorTaskUtils._
import org.gridgain.grid.kernal.visor.cmd.tasks.VisorCachesClearTask
import org.gridgain.visor.commands.VisorTextTable
import org.gridgain.visor.visor._

import scala.collection.JavaConversions._
import scala.language.reflectiveCalls
import scala.util.control.Breaks._

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
 *         Clears interactively selected cache.
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

        val cacheName = cacheArg match {
            case None => null // default cache.

            case Some(s) if s.startsWith("@") =>
                warn("Can't find cache variable with specified name: " + s,
                    "Type 'cache' to see available cache variables."
                )

                break()

            case Some(name) => name
        }

        val prj = if (node.isDefined) grid.forNode(node.get) else grid.forCache(cacheName)

        if (prj.nodes().isEmpty) {
            val msg =
                if (cacheName == null)
                    "Can't find nodes with default cache."
                else
                    "Can't find nodes with specified cache: " + cacheName

            scold(msg).^^
        }

        val t = VisorTextTable()

        t #= ("Node ID8(@)", "Entries Cleared", "Cache Size Before", "Cache Size After")

        val cacheSet = Collections.singleton(cacheName)

        prj.nodes().foreach(node => {
            val res = grid.forNode(node)
                .compute()
                .withName("visor-cclear-task")
                .withNoFailover()
                .execute(classOf[VisorCachesClearTask], toTaskArgument(node.id(), cacheSet))
                .get(cacheName)

            t += (nodeId8(node.id()), res.get1() - res.get2(), res.get1(), res.get2())
        })

        println("Cleared cache with name: " + escapeName(cacheName))

        t.render()
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
