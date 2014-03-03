/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */
package org.gridgain.visor.commands.cswap

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
 * Visor 'cswap' command implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to properly
 * import all necessary typed and implicit conversions:
 * <ex>
 * import org.gridgain.visor._
 * import commands.cswap.VisorCacheSwapCommand._
 * </ex>
 * Note that `VisorCacheSwapCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
 *
 * ==Help==
 * {{{
 * +-----------------------------------------------------+
 * | cswap | Swaps backup entries in cache on all nodes. |
 * +-----------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     cswap
 *     cswap "<cache-name>"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     <cache-name>
 *         Name of the cache.
 *         If not specified, entries in default cache will be swapped.
 * }}}
 *
 * ====Examples====
 * {{{
 *     cswap
 *         Swaps entries in default cache.
 *     cswap "cache"
 *         Swaps entries in cache with name 'cache'.
 * }}}
 *
 * @author @java.author
 * @version @java.version
 */
class VisorCacheSwapCommand {
    /**
     * Prints error message and advise.
     *
     * @param errMsgs Error messages.
     */
    private def scold(errMsgs: Any*) {
        assert(errMsgs != null)

        warn(errMsgs: _*)
        warn("Type 'help cswap' to see how to use this command.")
    }

    /**
     * ===Command===
     * Swaps entries in cache.
     *
     * ===Examples===
     * <ex>cswap "cache"</ex>
     * Swaps entries in cache with name 'cache'.
     *
     * @param cacheName Cache name.
     */
    def cswap(@Nullable cacheName: String) = breakable {
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
                .withName("visor-cswap-task")
                .withNoFailover()
                .broadcast(new SwapCommand(caches)).get

            val t = VisorTextTable()

            t #= ("Node ID8(@)", "Entries Swapped", "Cache Size Before", "Cache Size After")

            res.foreach(r => t += (nodeId8(r._1), r._2, r._3, r._4))

            t.render()
        }
    }

    /**
     * ===Command===
     * Swaps entries in default cache.
     *
     * ===Examples===
     * <ex>cswap</ex>
     * Swaps entries in default cache.
     */
    def cswap() {
        cswap(null)
    }
}

/**
 * @author @java.author
 * @version @java.version
 */
@GridInternal
class SwapCommand(val cacheName: String) extends CO[(UUID, Int, Int, Int)] {
    @GridInstanceResource
    private val g: Grid = null

    @impl def apply(): (UUID, Int, Int, Int) = {
        val c = g.asInstanceOf[GridEx].cachex[AnyRef, AnyRef](cacheName)

        val oldSize = c.size

        val cnt = (c.entrySet :\ 0)((e, cnt) => if (e.backup && e.evict()) cnt + 1 else cnt)

        (g.localNode.id, cnt, oldSize, c.size)
    }
}

/**
 * Companion object that does initialization of the command.
 *
 * @author @java.author
 * @version @java.version
 */
object VisorCacheSwapCommand {
    addHelp(
        name = "cswap",
        shortInfo = "Swaps backup entries in cache on all nodes.",
        spec = List(
            "cswap",
            "cswap <cache-name>"
        ),
        args = List(
            "<cache-name>" -> List(
                "Name of the cache.",
                "If not specified, entries in default cache will be swapped.",
                "Note you can also use '@c0' ... '@cn' variables as shortcut to <cache-name>."
            )
        ),
        examples = List(
            "cswap" -> "Swaps entries in default cache.",
            "cswap cache" -> "Swaps entries in cache with name 'cache'.",
            "cswap @c0" -> "Swaps entries in cache with name taken from 'c0' memory variable."
        ),
        ref = VisorConsoleCommand(cmd.cswap, cmd.cswap)
    )

    /** Singleton command. */
    private val cmd = new VisorCacheSwapCommand

    /**
     * Singleton.
     */
    def apply() = cmd

    /**
     * Implicit converter from visor to commands "pimp".
     *
     * @param vs Visor tagging trait.
     */
    implicit def fromCSwap2Visor(vs: VisorTag) = cmd
}
