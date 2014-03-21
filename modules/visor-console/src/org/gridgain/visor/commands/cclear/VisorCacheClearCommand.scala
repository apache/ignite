/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */
package org.gridgain.visor.commands.cclear

import java.util.UUID
import org.jetbrains.annotations.Nullable
import scala.collection.JavaConversions._
import scala.util.control.Breaks._
import org.gridgain.scalar._
import scalar._
import org.gridgain.grid.kernal.GridEx
import org.gridgain.grid.kernal.processors.task.GridInternal
import org.gridgain.grid.util.scala.impl
import org.gridgain.grid.resources._
import org.gridgain.visor._
import org.gridgain.visor.commands.{VisorConsoleCommand, VisorTextTable}
import visor._
import org.gridgain.grid.lang.GridCallable

/**
 * ==Overview==
 * Visor 'cclear' command implementation.
 *
 * ==Help==
 * {{{
 * +------------------------------------------------------+
 * | cclear | Clears all entries from cache on all nodes. |
 * +------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     cclear
 *     cclear -c=<cache-name>
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
 *     cclear
 *         Clears default cache.
 *     cclear -c=cache
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
        warn("Type 'help cclear' to see how to use this command.")
    }

    /**
     * ===Command===
     * Clears cache by its name.
     *
     * ===Examples===
     * <ex>cclear -c=cache</ex>
     * Clears cache with name 'cache'.
     *
     * @param args Command arguments.
     */
    def cclear(@Nullable args: String) = breakable {
        if (!isConnected)
            adviseToConnect()
        else {
            val argLst = parseArgs(args)

            val cacheArg = argValue("c", argLst)

            if (cacheArg.isEmpty)
                scold("Cache name is empty.").^^

            val caches = getVariable(cacheArg.get)

            val prj = grid.forCache(caches)

            if (prj.isEmpty) {
                val msg =
                    if (caches == null)
                        "Can't find nodes with default cache."
                    else
                        "Can't find nodes with specified cache: " + caches

                scold(msg).^^
            }

            val res = prj
                .compute()
                .withName("visor-cclear-task")
                .withNoFailover()
                .broadcast(new ClearClosure(caches))
                .get

            val t = VisorTextTable()

            t #= ("Node ID8(@)", "Cache Size Before", "Cache Size After")

            res.foreach(r => t += (nodeId8(r._1), r._2, r._3))

            t.render()
        }
    }

    /**
     * ===Command===
     * Clears default cache.
     *
     * ===Examples===
     * <ex>cclear</ex>
     * Clears default cache.
     */
    def cclear() {
        cclear(null)
    }
}

/**
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
    addHelp(
        name = "cclear",
        shortInfo = "Clears all entries from cache on all nodes.",
        spec = Seq(
            "cclear",
            "cclear -c=<cache-name>"
        ),
        args = Seq(
            "-c=<cache-name>" -> Seq(
                "Name of the cache.",
                "If not specified, default cache will be cleared.",
                "Note you can also use '@c0' ... '@cn' variables as shortcut to <cache-name>."
            )
        ),
        examples = Seq(
            "cclear" -> "Clears default cache.",
            "cclear -c=cache" -> "Clears cache with name 'cache'.",
            "cclear -c=@c0" -> "Clears cache with name taken from 'c0' memory variable."
        ),
        ref = VisorConsoleCommand(cmd.cclear, cmd.cclear)
    )

    /** Singleton command. */
    private val cmd = new VisorCacheClearCommand

    /**
     * Singleton.
     */
    def apply() = cmd

    /**
     * Implicit converter from visor to commands "pimp".
     *
     * @param vs Visor tagging trait.
     */
    implicit def fromCClear2Visor(vs: VisorTag) = cmd
}
