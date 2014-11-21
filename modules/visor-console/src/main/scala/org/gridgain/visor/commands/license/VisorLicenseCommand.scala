/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands.license

import org.gridgain.grid._

import java.io._

import org.gridgain.visor._
import org.gridgain.visor.commands.VisorConsoleCommand
import org.gridgain.visor.visor._

import scala.language.implicitConversions

/**
 * ==Overview==
 * Contains Visor command `license` implementation.
 *
 * ==Help==
 * {{{
 * +---------------------------------------------------------------------------+
 * | license | Shows information about all licenses that are used on the grid. |
 * |         | Also can be used to update one of the licenses.                  |
 * +---------------------------------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     license
 *     license "-f=<path> -id=<license-id>"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -f=<path>
 *         Path to new license XML file.
 *     -id=<license-id>
 *         ID of the license will be updated.
 * }}}
 *
 * ====Examples====
 * {{{
 *     license
 *         Shows all licenses that are used on the grid.
 *     license "-f=/path/to/new/license.xml -id=fbdea781-90e6-4d1b-b8b3-5b8c14aa2df7"
 *         Copies new license file to all nodes that use license with provided ID.
 * }}}
 */
class VisorLicenseCommand {
    /**
     * Prints error message and advise.
     *
     * @param errMsgs Error messages.
     */
    private def scold(errMsgs: Any*) {
        assert(errMsgs != null)

        nl()

        warn(errMsgs: _*)
        warn("Type 'help license' to see how to use this command.")
    }

    /**
     * ===Command===
     * Shows all licenses that are used on the grid.
     *
     * ===Examples===
     * <ex>license</ex>
     * Shows all licenses that are used on the grid.
     */
    def license() {
        if (!isConnected)
            adviseToConnect()
        else {
            if (grid.nodes().isEmpty)
                scold("Topology is empty.")
            else {
                val nodes = grid.nodes()

//                val lics = try
//                    grid.compute(grid.forNodes(nodes)).execute(classOf[VisorLicenseCollectTask],
//                        emptyTaskArgument(nodes.map(_.id())))
//                        .groupBy(n => Option(n.get2()).fold("Open source")(_.id().toString))
//                catch {
//                    case _: GridException =>
//                        warn("Failed to obtain license from grid.")
//
//                        return
//                }

//                if (lics.nonEmpty) {
//                    lics.foreach(e => {
//                        val l = e._2.head.get2()
//
//                        nl()
//
//                        println("License ID: '" + e._1 + "'")
//
//                        if (l != null) {
//                            val licT = new VisorTextTable()
//
//                            licT += ("Version regex", safe(l.versionRegexp(), "<n/a>"))
//                            licT += ("Issue date", Option(l.issueDate()).fold("<n/a>")(d => formatDate(d)))
//                            licT += ("License note", safe(l.note(), "<n/a>"))
//
//                            val gracePeriod = if (l.gracePeriodLeft < 0)
//                                    "No grace/burst period"
//                                else if (l.gracePeriodLeft > 0)
//                                    s"License grace/burst period - left ${U.formatMins(l.gracePeriodLeft)}."
//                                else
//                                    "License grace/burst period expired."
//
//                            licT += ("Grace/burst period", gracePeriod)
//                            licT += ("Licensee name", safe(l.userName(), "<n/a>"))
//                            licT += ("Licensee organization", safe(l.userOrganization(), "<n/a>"))
//                            licT += ("Licensee URL", safe(l.userWww(), "<n/a>"))
//                            licT += ("Licensee e-mail", safe(l.userEmail(), "<n/a>"))
//                            licT += ("Maximum number of nodes", if (l.maxNodes() > 0) l.maxNodes() else "No restriction")
//                            licT += ("Maximum number of hosts", if (l.maxComputers() > 0) l.maxComputers() else "No restriction")
//                            licT += ("Maximum number of CPUs", if (l.maxCpus() > 0) l.maxCpus() else "No restriction")
//                            licT += ("Maximum up time", if (l.maxUpTime() > 0) U.formatMins(l.maxUpTime()) else "No restriction")
//                            licT += ("Maintenance time",
//                            if (l.maintenanceTime() > 0) l.maintenanceTime() + " months" else "No restriction")
//                            licT += ("Expire date", Option(l.expireDate()).fold("No restriction")(d => formatDate(d)))
//                            licT += ("Disabled subsystems", Option(l.disabledSubsystems()).
//                                fold("No disabled subsystems")(s => s.split(',').toList.toString()))
//
//                            licT.render()
//                        }
//
//                        nl()
//
//                        println("Nodes with license '" + e._1 + "':")
//
//                        val sumT = new VisorTextTable()
//
//                        sumT #= ("Node ID8(@)", "License ID")
//
//                        e._2.foreach(t => sumT += (nodeId8Addr(t.get1()), Option(t.get2()).fold("Open source")(_.id().toString)))
//
//                        sumT.render()
//                    })
//                }
            }
        }
    }

    /**
     * ===Command===
     * Updates license with provided ID.
     *
     * ===Examples===
     * <ex>license "-f=/path/to/new/license.xml -id=fbdea781-90e6-4d1b-b8b3-5b8c14aa2df7"</ex>
     * Copies new license file to all nodes that use license with provided ID.
     *
     * @param args Command arguments.
     */
    def license(args: String) {
        assert(args != null)

        val argLst = parseArgs(args)

        val path = argValue("f", argLst)
        val id = argValue("id", argLst)

        if (!path.isDefined)
            scold("Path to new license file is not defined.")
        else if (!id.isDefined)
            scold("Old license ID is not defined.")
        else {
            val licId = id.get
            val licPath = path.get

            try {
                val nodes = grid.nodes()

//                nodes.foreach(n => {
//                    grid.compute(grid.forNode(n)).withNoFailover().
//                        execute(classOf[VisorLicenseUpdateTask], toTaskArgument(n.id,
//                        new GridBiTuple(UUID.fromString(licId), Source.fromFile(licPath).mkString)))
//                })

                println("All licenses have been updated.")

                nl()

                license()
            }
            catch {
                case _: IllegalArgumentException => scold("Invalid License ID: " + licId)
                case _: FileNotFoundException => scold("File not found: " + licPath)
                case _: IOException => scold("Failed to read the license file: " + licPath)
                case _: GridException => scold(
                    "Failed to update the license due to system error.",
                    "Note: Some licenses may haven been updated."
                )
            }
        }
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorLicenseCommand {
    addHelp(
        name = "license",
        shortInfo = "Shows information about licenses and updates them.",
        longInfo = List(
            "Shows information about all licenses that are used on the grid.",
            "Also can be used to update on of the licenses."
        ),
        spec = List(
            "license",
            "license -f=<path> -id=<license-id>"
        ),
        args = List(
            "-f=<path>" -> "Path to new license XML file.",
            "-id=<license-id>" -> "ID of the license will be updated."
        ),
        examples = List(
            "license" ->
                "Shows all licenses that are used on the grid.",
            "license -f=/path/to/new/license.xml -id=fbdea781-90e6-4d1b-b8b3-5b8c14aa2df7" ->
                "Copies new license file to all nodes that use license with provided ID."
        ),
        ref = VisorConsoleCommand(cmd.license, cmd.license)
    )

    /** Singleton command. */
    private val cmd = new VisorLicenseCommand

    /**
     * Singleton.
     */
    def apply() = cmd

    /**
     * Implicit converter from visor to commands "pimp".
     *
     * @param vs Visor tagging trait.
     */
    implicit def fromLicense2Visor(vs: VisorTag): VisorLicenseCommand = cmd
}
