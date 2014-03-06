// @scala.file.header

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.cscan

import org.gridgain.visor._
import org.gridgain.visor.commands.{VisorTextTable, VisorConsoleCommand}
import org.gridgain.visor.visor._

/**
 * ==Overview==
 * Visor 'cscan' command implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to properly
 * import all necessary typed and implicit conversions:
 * <ex>
 * import org.gridgain.visor._
 * import commands.cscan.VisorCacheScanCommand._
 * </ex>
 * Note that `VisorCacheScanCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
 *
 * ==Help==
 * {{{
 * +------------------------------------------------------+
 * | cscan | List all entries from cache on all nodes.    |
 * +------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     visor cscan
 *     visor cscan "<cache-name>"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     <cache-name>
 *         Name of the cache.
 * }}}
 *
 * ====Examples====
 * {{{
 *     visor cscan "cache"
 *         List all entries in cache with name 'cache'.
 * }}}
 */
class VisorCacheScanCommand {
    /**
     * Prints error message and advise.
     *
     * @param errMsgs Error messages.
     */
    private def scold(errMsgs: Any*) {
        assert(errMsgs != null)

        warn(errMsgs: _*)
        warn("Type 'help cscan' to see how to use this command.")
    }

    /**
     * ===Command===
     * List all entries in cache with specified name.
     *
     * ===Examples===
     * <ex>cscan "cache"</ex>
     * List all entries in cache with name 'cache'.
     *
     * @param cacheName Cache name.
     */
    def cscan(cacheName: String) = breakable {
        if (!isConnected)
            adviseToConnect()
        else {
            if (cacheName == null || cacheName.isEmpty)
                scold("Cache name is empty.").^^

            val caches = getVariable(cacheName)

            val prj = grid.forCache(caches)

            if (prj.isEmpty)
                scold("Can't find nodes with specified cache: " + caches).^^

//            val res = prj
//                .compute()
//                .withName("visor-cscan-task")
//                .withNoFailover()
//                .broadcast(new ClearClosure(caches))
//                .get
//
            val t = VisorTextTable()

            t #= ("Node ID8(@)", "Cache Size Before", "Cache Size After")

//            res.foreach(r => t += (nodeId8(r._1), r._2, r._3))

            t.render()
        }
    }
}

///**
//  */
//@GridInternal
//class ClearClosure(val cacheName: String) extends GridCallable[(UUID, Int, Int)] {
//    @GridInstanceResource
//    private val g: GridEx = null
//
//    @impl def call(): (UUID, Int, Int) = {
//        val c = g.cachex[AnyRef, AnyRef](cacheName)
//
//        val oldSize = c.size
//
//        c.clearAll()
//
//        (g.localNode.id, oldSize, c.size)
//    }
//}

/**
 * Companion object that does initialization of the command.
 */
object VisorCacheScanCommand {
    addHelp(
        name = "cscan",
        shortInfo = "List all entries from cache on all nodes.",
        spec = Seq(
            "cscan <cache-name>"
        ),
        args = Seq(
            "<cache-name>" -> Seq(
                "Name of the cache.",
                "Note you can also use '@c0' ... '@cn' variables as shortcut to <cache-name>."
            )
        ),
        examples = Seq(
            "cscan cache" -> "List entries from cache with name 'cache'.",
            "cscan @c0" -> "List entries from cache with name taken from 'c0' memory variable."
        ),
        ref = VisorConsoleCommand(cmd.cscan, cmd.cscan)
    )

    /** Singleton command. */
    private val cmd = new VisorCacheScanCommand

    /**
     * Singleton.
     */
    def apply() = cmd

    /**
     * Implicit converter from visor to commands "pimp".
     *
     * @param vs Visor tagging trait.
     */
    implicit def fromcscan2Visor(vs: VisorTag) = cmd
}
