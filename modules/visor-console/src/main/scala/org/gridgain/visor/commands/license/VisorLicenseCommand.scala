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

import org.gridgain.scalar._
import scalar._
import org.gridgain.visor._
import org.gridgain.visor.commands.{VisorConsoleCommand, VisorTextTable}
import visor._
import org.gridgain.grid._
import resources.GridInstanceResource
import org.jetbrains.annotations.Nullable
import java.text.SimpleDateFormat
import java.io._
import scala.io.Source
import java.util.{Locale, Date, UUID}
import java.net.URL
import org.gridgain.grid.lang.{GridCallable, GridRunnable}

/**
 * License data.
 */
private case class License(
    id: String,
    ver: String,
    verRegexp: String,
    issueDate: String,
    maintenanceTime: String,
    issueOrg: String,
    userName: String,
    userOrg: String,
    userWww: String,
    userEmail: String,
    note: String,
    expDate: String,
    maxNodes: String,
    maxComp: String,
    maxCpus: String,
    maxUpTime: String,
    gracePeriod: String,
    disSubs: String
) {
    override def equals(r: Any) =
        if (this eq r.asInstanceOf[AnyRef])
            true
        else if (r == null || !r.isInstanceOf[License])
            false
        else
            r.asInstanceOf[License].id == id

    override def hashCode() =
        id.hashCode()
}

/**
 * License getter closure.
 */
private class LicenseGetter extends GridCallable[License] {
    /**Injected grid */
    @GridInstanceResource
    private val g: Grid = null

    override def call(): License = {
        val l = g.product().license()

        if (l == null)
            null
        else {
            License(
                id = safe(l.id, "<n/a>"),
                ver = safe(l.version, "<n/a>"),
                verRegexp = safe(l.versionRegexp, "<n/a>"),
                issueDate =
                    if (l.issueDate != null)
                        formatDate(l.issueDate)
                    else
                        "<n/a>",
                maintenanceTime =
                    if (l.maintenanceTime > 0)
                        l.maintenanceTime.toString + " months"
                    else
                        "No restriction",
                issueOrg = safe(l.issueOrganization, "<n/a>"),
                userName = safe(l.userName, "<n/a>"),
                userOrg = safe(l.userOrganization, "<n/a>"),
                userWww = safe(l.userWww, "<n/a>"),
                userEmail = safe(l.userEmail, "<n/a>"),
                note = safe(l.licenseNote, "<n/a>"),
                expDate =
                    if (l.expireDate != null)
                        formatDate(l.expireDate)
                    else
                        "No restriction",
                maxNodes =
                    if (l.maxNodes > 0)
                        l.maxNodes.toString
                    else
                        "No restriction",
                maxComp =
                    if (l.maxComputers > 0)
                        l.maxComputers.toString
                    else
                        "No restriction",
                maxCpus =
                    if (l.maxCpus > 0)
                        l.maxCpus.toString
                    else
                        "No restriction",
                maxUpTime =
                    if (l.maxUpTime > 0)
                        l.maxUpTime + " min."
                    else
                        "No restriction",
                gracePeriod =
                    if (l.gracePeriod > 0)
                        l.maxUpTime + " min."
                    else
                        "No grace/burst period",
                disSubs = safe(l.disabledSubsystems, "No disabled subsystems")
            )
        }
    }

    /** Date format. */
    private val dFmt = new SimpleDateFormat("MM/dd/yy", Locale.US)

    /**
     * Gets a non-`null` value for given parameter.
     *
     * @param a Parameter.
     * @param dflt Value to return if `a` is `null`.
     */
    private def safe(@Nullable a: Any, dflt: Any = ""): String = {
        assert(dflt != null)

        if (a != null)
            a.toString
        else
            dflt.toString
    }

    /**
     * Returns string representation of the timestamp provided. Result formatted
     * using pattern `MM/dd/yy`.
     *
     * @param date Timestamp.
     */
    private def formatDate(date: Date): String =
        dFmt.format(date)
}

/**
 * License updater closure.
 */
private class LicenseUpdater(oldLicId: UUID, newLicLines: List[String]) extends GridRunnable {
    /**Injected grid */
    @GridInstanceResource
    private val g: Grid = null

    override def run() {
        val lic = g.product().license()

        if (lic != null && lic.id == oldLicId) {
            val file = new File(new URL(g.configuration().getLicenseUrl).toURI)

            val writer = new BufferedWriter(new FileWriter(file, false))

            newLicLines.foreach(l => {
                writer.write(l)
                writer.newLine()
            })

            writer.close()
        }
    }
}

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
            if (grid.isEmpty)
                scold("Topology is empty.")
            else {
                val f = new LicenseGetter()

                var ls = Set.empty[License]

                val sumT = new VisorTextTable()

                sumT #= ("Node ID8(@)", "License ID")

                grid.foreach(n => {
                    try {
                        val lic = grid.forNode(n).compute().withNoFailover().call(f).get

                        val id = lic.id

                        sumT += (nodeId8Addr(n.id), id)

                        ls += lic
                    }
                    catch {
                        case _: GridException => warn("Failed to obtain license from: " + nodeId8Addr(n.id))
                    }
                })

                if (!ls.isEmpty) {
                    sumT.render()

                    ls.foreach(l => {
                        nl()

                        println("License '" + l.id + "':")

                        val licT = new VisorTextTable()

                        licT += ("Version", l.ver)
                        licT += ("Version regular expression", l.verRegexp)
                        licT += ("Issue date", l.issueDate)
                        licT += ("Maintenance time", l.maintenanceTime)
                        licT += ("Issue organization", l.issueOrg)
                        licT += ("User name", l.userName)
                        licT += ("User organization", l.userOrg)
                        licT += ("User organization URL", l.userWww)
                        licT += ("User organization e-mail", l.userEmail)
                        licT += ("License note", l.note)
                        licT += ("Expire date", l.expDate)
                        licT += ("Maximum number of nodes", l.maxNodes)
                        licT += ("Maximum number of computers", l.maxComp)
                        licT += ("Maximum number of CPUs", l.maxCpus)
                        licT += ("Maximum up time", l.maxUpTime)
                        licT += ("Grace/burst period", l.gracePeriod)
                        licT += ("Disabled subsystems", l.disSubs.split(',').toList)

                        licT.render()
                    })
                }
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
                grid.compute()
                    .withNoFailover()
                    .broadcast(new LicenseUpdater(UUID.fromString(licId), Source.fromFile(licPath).getLines().toList))

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
                    "Note: some licenses may haven been updated."
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
    implicit def fromLicense2Visor(vs: VisorTag) = cmd
}
