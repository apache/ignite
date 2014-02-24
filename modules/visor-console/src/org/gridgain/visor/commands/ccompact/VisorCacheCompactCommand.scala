// @scala.file.header

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.ccompact

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
import org.gridgain.grid.util.typedef._
import util.scala.impl
import org.gridgain.grid.kernal.processors.task.GridInternal

/**
 * ==Overview==
 * Visor 'ccompact' command implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to properly
 * import all necessary typed and implicit conversions:
 * <ex>
 * import org.gridgain.visor._
 * import commands.ccompact.VisorCacheCompactCommand._
 * </ex>
 * Note that `VisorCacheCompactCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
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
 *     ccompact "<cache-name>"
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
 *     ccompact "cache"
 *         Compacts entries in cache with name 'cache'.
 * }}}
 *
 * @author @java.author
 * @version @java.version
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
        warn("Type 'help ccompact' to see how to use this command.")
    }

    /**
     * ===Command===
     * Compacts entries in cache.
     *
     * ===Examples===
     * <ex>ccompact "cache"</ex>
     * Compacts entries in cache with name 'cache'.
     *
     * @param cacheName Cache name.
     */
    def ccompact(@Nullable cacheName: String) = breakable {
        if (!isConnected)
            adviseToConnect()
        else {
            if (cacheName != null && cacheName.isEmpty)
                scold("Cache name is empty.").^^

            val caches = getVariable(cacheName)

            val prj = grid.forCache(caches)

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
                .call(new CompactClosure(caches)).get

            val t = VisorTextTable()

            t #= ("Node ID8(@)", "Entries Compacted", "Cache Size Before", "Cache Size After")

            t += (nodeId8(res._1), res._2, res._3, res._4)

            t.render()
        }
    }

    /**
     * ===Command===
     * Compacts entries in default cache.
     *
     * ===Examples===
     * <ex>ccompact</ex>
     * Compacts entries in default cache.
     */
    def ccompact() {
        ccompact(null)
    }
}

/**
 * @author @java.author
 * @version @java.version
 */
@GridInternal
class CompactClosure(val cacheName: String) extends CO[(UUID, Int, Int, Int)] {
    @GridInstanceResource
    private val g: Grid = null

    @impl def apply(): (UUID, Int, Int, Int) = {
        val c = g.asInstanceOf[GridEx].cachex[AnyRef, AnyRef](cacheName)

        val oldSize = c.size

        val cnt = (c.keySet :\ 0)((k, cnt) => if (c.compact(k)) cnt + 1 else cnt)

        (g.localNode.id, cnt, oldSize, c.size)
    }
}

/**
 * Companion object that does initialization of the command.
 *
 * @author @java.author
 * @version @java.version
 */
object VisorCacheCompactCommand {
    addHelp(
        name = "ccompact",
        shortInfo = "Compacts all entries in cache on all nodes.",
        spec = List(
            "ccompact",
            "ccompact <cache-name>"
        ),
        args = List(
            "<cache-name>" -> List(
                "Name of the cache.",
                "If not specified, entries in default cache will be compacted.",
                "Note you can also use '@c0' ... '@cn' variables as shortcut to <cache-name>."
            )
        ),
        examples = List(
            "ccompact" -> "Compacts entries in default cache.",
            "ccompact cache" -> "Compacts entries in cache with name 'cache'.",
            "ccompact @c0" -> "Compacts cache with name taken from 'c0' memory variable."
        ),
        ref = VisorConsoleCommand(cmd.ccompact, cmd.ccompact)
    )

    /** Singleton command. */
    private val cmd = new VisorCacheCompactCommand

    /**
     * Singleton.
     */
    def apply() = cmd

    /**
     * Implicit converter from visor to commands "pimp".
     *
     * @param vs Visor tagging trait.
     */
    implicit def fromCCompact2Visor(vs: VisorTag) = cmd
}
