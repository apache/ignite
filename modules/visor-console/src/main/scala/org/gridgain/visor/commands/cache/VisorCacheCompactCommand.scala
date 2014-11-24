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

import org.gridgain.grid.kernal.visor.tasks.cache.VisorCacheCompactTask
import org.gridgain.grid.kernal.visor.util.VisorTaskUtils

import java.util.{Collections, HashSet => JavaHashSet}

import org.gridgain.grid._
import VisorTaskUtils._
import org.gridgain.visor.commands.VisorTextTable
import org.gridgain.visor.visor._

import scala.collection.JavaConversions._
import scala.language.reflectiveCalls
import scala.util.control.Breaks._

/**
 * ==Overview==
 * Visor 'compact' command implementation.
 *
 * ==Help==
 * {{{
 * +--------------------------------------------------------+
 * | compact | Compacts all entries in cache on all nodes. |
 * +--------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     compact
 *     compact -c=<cache-name>
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
 *     compact
 *         Compacts entries in interactively selected cache.
 *     compact -c=cache
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

        t #= ("Node ID8(@)", "Entries Compacted", "Cache Size Before", "Cache Size After")

        val cacheSet = Collections.singleton(cacheName)

        prj.nodes().foreach(node => {
            val r = grid.compute(grid.forNode(node))
                .withName("visor-ccompact-task")
                .withNoFailover()
                .execute(classOf[VisorCacheCompactTask], toTaskArgument(node.id(), cacheSet))
                .get(cacheName)

            t += (nodeId8(node.id()), r.get1() - r.get2(), r.get1(), r.get2())
        })

        println("Compacts entries in cache: " + escapeName(cacheName))

        t.render()
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
