/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.visor.commands.deploy

import org.apache.ignite.internal.util.io.GridFilenameUtils
import org.apache.ignite.internal.util.typedef.X
import org.apache.ignite.internal.util.{IgniteUtils => U}

import com.jcraft.jsch._

import java.io._
import java.net.UnknownHostException
import java.util.concurrent._

import org.apache.ignite.visor.VisorTag
import org.apache.ignite.visor.commands.VisorConsoleCommand
import org.apache.ignite.visor.visor._

import scala.language.{implicitConversions, reflectiveCalls}
import scala.util.control.Breaks._

/**
 * Host data.
 */
private case class VisorHost(
    name: String,
    port: Int,
    uname: String,
    passwd: Option[String]
) {
    assert(name != null)
    assert(port > 0)
    assert(uname != null)
    assert(passwd != null)

    override def equals(r: Any) =
        if (this eq r.asInstanceOf[AnyRef])
            true
        else if (r == null || !r.isInstanceOf[VisorHost])
            false
        else
            r.asInstanceOf[VisorHost].name == name

    override def hashCode() =
        name.hashCode()
}

/**
 * Runnable that copies file or directory.
 */
private case class VisorCopier(
    host: VisorHost,
    key: Option[String],
    src: String,
    dest: String
) extends Runnable {
    assert(host != null)
    assert(key != null)
    assert(src != null)
    assert(dest != null)

    assert(host.passwd.isDefined || key.isDefined)
    assert(!(host.passwd.isDefined && key.isDefined))

    /** SSH session. */
    private var ses: Session = null

    override def run() {
        assert(ses == null)

        val ssh = new JSch

        if (key.isDefined)
            ssh.addIdentity(key.get)

        ses = ssh.getSession(host.uname, host.name, host.port)

        if (host.passwd.isDefined)
            ses.setPassword(host.passwd.get)

        ses.setConfig("StrictHostKeyChecking", "no")

        try {
            ses.connect()

            var ch: ChannelSftp = null

            try {
                val ggh = ggHome()

                if (ggh == "")
                    warn("IGNITE_HOME is not set on " + host.name)
                else {
                    ch = ses.openChannel("sftp").asInstanceOf[ChannelSftp]

                    ch.connect()

                    copy(ch, src, GridFilenameUtils.separatorsToUnix(ggh + "/" + dest))

                    println("ok => " + host.name)
                }
            }
            finally {
                if (ch != null && ch.isConnected)
                    ch.disconnect()
            }
        }
        catch {
            case e: JSchException if X.hasCause(e, classOf[UnknownHostException]) =>
                println("Visor Console failed to deploy. Reason: unknown host - " + host.name)

            case e: JSchException =>
                println("Visor Console failed to deploy. Reason: " + e.getMessage)

            case e: Exception =>
                warn(e.getMessage)
        }
        finally {
            if (ses.isConnected)
                ses.disconnect()
        }
    }

    /**
     * Gets `IGNITE_HOME` from remote host.
     *
     * @return `IGNITE_HOME` value.
     */
    private def ggHome(): String = {
        /**
         * Non interactively execute command.
         *
         * @param cmd command.
         * @return command results
         */
        def exec(cmd: String) = {
            val ch = ses.openChannel("exec").asInstanceOf[ChannelExec]

            try {
                ch.setCommand(cmd)

                ch.connect()

                new BufferedReader(new InputStreamReader(ch.getInputStream)).readLine
            }
            catch {
                case e: JSchException =>
                    warn(e.getMessage)

                    ""
            }
            finally {
                if (ch.isConnected)
                    ch.disconnect()
            }
        }

        /**
         * Interactively execute command.
         *
         * @param cmd command.
         * @return command results.
         */
        def shell(cmd: String): String = {
            val ch = ses.openChannel("shell").asInstanceOf[ChannelShell]

            try {
                ch.connect()

                // Added to skip login message.
                U.sleep(1000)

                val writer = new PrintStream(ch.getOutputStream, true)

                val reader = new BufferedReader(new InputStreamReader(ch.getInputStream))

                // Send command.
                writer.println(cmd)

                // Read echo command.
                reader.readLine()

                // Read command result.
                reader.readLine()
            }
            catch {
                case e: JSchException =>
                    warn(e.getMessage)

                    ""
            }
            finally {
                if (ch.isConnected)
                    ch.disconnect()
            }
        }

        /**
         * Checks whether host is running Windows OS.
         *
         * @return Whether host is running Windows OS.
         * @throws JSchException In case of SSH error.
         */
        def windows = {
            try
                exec("cmd.exe") != null
            catch {
                case ignored: IOException => false
            }
        }

        if (windows)
            exec("echo %IGNITE_HOME%")
        else
            shell("echo $IGNITE_HOME") // Use interactive shell under nix because need read env from .profile and etc.
    }

    /**
     * Copies file or directory.
     *
     * @param ch SFTP channel.
     * @param src Source path.
     * @param dest Destination path.
     */
    private def copy(ch: ChannelSftp, src: String, dest: String) {
        assert(ch != null)
        assert(src != null)
        assert(dest != null)

        val root = new File(src)

        if (!root.exists)
            throw new Exception("File or folder not found: " + src)

        try {
            if (root.isDirectory) {
                try
                    ch.ls(dest)
                catch {
                    case _: SftpException => ch.mkdir(dest)
                }

                root.listFiles.foreach(
                    f => copy(ch, f.getPath, GridFilenameUtils.separatorsToUnix(dest + "/" + f.getName)))
            }
            else
                ch.put(src, dest)
        }
        catch {
            case e: SftpException =>
                println("Visor Console failed to deploy from: " + src + " to: " + dest + ". Reason: " + e.getMessage)
            case e: IOException =>
                println("Visor Console failed to deploy from: " + src + " to: " + dest + ". Reason: " + e.getMessage)
        }
    }
}

/**
 * ==Overview==
 * Visor 'deploy' command implementation.
 *
 * ==Help==
 * {{{
 * +---------------------------------------------------+
 * | deploy | Copies file or directory to remote host. |
 * |        | Command relies on SFTP protocol.         |
 * +---------------------------------------------------+
 * }}}
 *
 * ====Specification====
 * {{{
 *     deploy "-h={<username>{:<password>}@}<host>{:<port>} {-u=<username>}
 *         {-p=<password>} {-k=<path>} -s=<path> {-d<path>}"
 * }}}
 *
 * ====Arguments====
 * {{{
 *     -h={<username>{:<password>}@}<host>{:<port>}
 *         Host specification.
 *
 *         <host> can be a hostname, IP or range of IPs.
 *         Example of range is 192.168.1.100~150,
 *         which means all IPs from 192.168.1.100 to 192.168.1.150 inclusively.
 *
 *         Default port number is 22.
 *
 *         This option can be provided multiple times.
 *     -u=<username>
 *         Default username.
 *         Used if specification doesn't contain username.
 *         If default is not provided as well, current local username will be used.
 *     -p=<password>
 *         Default password.
 *         Used if specification doesn't contain password.
 *         If default is not provided as well, it will be asked interactively.
 *     -k=<path>
 *         Path to private key file.
 *         If provided, it will be used for all specifications that doesn't contain password.
 *     -s=<path>
 *         Source path.
 *     -d=<path>
 *         Destination path (relative to IGNITE_HOME).
 *         If not provided, files will be copied to the root of IGNITE_HOME.
 * }}}
 *
 * ====Examples====
 * {{{
 *     deploy "-h=uname:passwd@host -s=/local/path -d=remote/path"
 *         Copies file or directory to remote host (password authentication).
 *     deploy "-h=uname@host -k=ssh-key.pem -s=/local/path -d=remote/path"
 *         Copies file or directory to remote host (private key authentication).
 * }}}
 */
class VisorDeployCommand {
    /** Default port. */
    private val DFLT_PORT = 22

    /** String that specifies range of IPs. */
    private val RANGE_SMB = "~"

    /**
     * Prints error message and advise.
     *
     * @param errMsgs Error messages.
     */
    private def scold(errMsgs: Any*) {
        assert(errMsgs != null)

        nl()

        warn(errMsgs: _*)
        warn("Type 'help deploy' to see how to use this command.")
    }

    /**
     * Catch point for missing arguments case.
     */
    def deploy() {
        scold("Missing arguments.")
    }

    /**
     * ===Command===
     * Copies file or directory to remote host.
     *
     * ===Examples===
     * <ex>deploy "-h=uname:passwd@host -s=/local/path -d=/remote/path"</ex>
     * Copies file or directory to remote host (password authentication).
     *
     * <ex>deploy "-h=uname@host -k=ssh-key.pem -s=/local/path -d=/remote/path"</ex>
     * Copies file or directory to remote host (private key authentication).
     */
    def deploy(args: String) = breakable {
        assert(args != null)

        val argLst = parseArgs(args)

        val dfltUname = argValue("u", argLst)
        val dfltPasswd = argValue("p", argLst)
        val key = argValue("k", argLst)
        val src = argValue("s", argLst)
        val dest = argValue("d", argLst)

        if (!src.isDefined)
            scold("Source is not defined.").^^

        var hosts = Set.empty[VisorHost]

        argLst.filter(_._1 == "h").map(_._2).foreach(h => {
            try
                hosts ++= mkHosts(h, dfltUname, dfltPasswd, key.isDefined)
            catch {
                case e: IllegalArgumentException => scold(e.getMessage).^^
            }
        })

        if (hosts.isEmpty)
            scold("At least one remote host should be specified.").^^

        val copiers = hosts.map(VisorCopier(_, key, src.get, dest getOrElse ""))

        try
            copiers.map(pool.submit(_)).foreach(_.get)
        catch {
            case _: RejectedExecutionException => scold("Failed due to system error.").^^
        }
    }

    /**
     * Parses host string.
     *
     * @param host Host string.
     * @param dfltUname `Option` for default username.
     * @param dfltPasswd `Option` for default password.
     * @param hasKey Whether private key file is defined.
     * @return Set of `Host` instances.
     */
    private def mkHosts(
        host: String,
        dfltUname: Option[String],
        dfltPasswd: Option[String],
        hasKey: Boolean): Set[VisorHost] = {
        assert(host != null)
        assert(dfltUname != null)
        assert(dfltPasswd != null)

        assert(host != null)
        assert(dfltUname != null)
        assert(dfltPasswd != null)

        var arr = host.split('@')

        def extractHostsPort(s: String) = {
            val hostPort = s.split(':')

            val hosts = expandHost(hostPort(0))

            val port =
                try
                    if (hostPort.size > 1) hostPort(1).toInt else DFLT_PORT
                catch {
                    case e: NumberFormatException =>
                        scold("Invalid port number: " + hostPort(1)).^^

                        // Never happens.
                        0
                }

            if (port <= 0)
                scold("Invalid port number: " + port).^^

            (hosts, port)
        }

        if (arr.size == 1) {
            val (hosts, port) = extractHostsPort(arr(0))

            val uname = dfltUname getOrElse System.getProperty("user.name")
            val passwd = if (!hasKey) Some(dfltPasswd getOrElse askPassword(uname)) else None

            hosts.map(VisorHost(_, port, uname, passwd))
        }
        else if (arr.size == 2) {
            val (hosts, port) = extractHostsPort(arr(1))

            arr = arr(0).split(':')

            val uname = arr(0)

            val passwd =
                if (arr.size > 1)
                    Some(arr(1))
                else if (!hasKey)
                    Some(dfltPasswd getOrElse askPassword(uname))
                else
                    None

            hosts.map(VisorHost(_, port, uname, passwd))
        }
        else {
            scold("Invalid host string: " + host).^^

            // Never happens.
            Set.empty
        }
    }

    /**
     * Parses and expands range of IPs, if needed. Host names without the range
     * returned as is.
     *
     * @param addr Host host with or without `~` range.
     * @return Set of individual host names (IPs).
     */
    private def expandHost(addr: String): Set[String] = {
        assert(addr != null)

        if (addr.contains(RANGE_SMB)) {
            val parts = addr.split(RANGE_SMB)

            if (parts.size != 2)
                scold("Invalid IP range: " + addr).^^

            val lastDot = parts(0).lastIndexOf('.')

            if (lastDot < 0)
                scold("Invalid IP range: " + addr).^^

            val (base, begin) = parts(0).splitAt(lastDot)
            val end = parts(1)

            try {
                val a = begin.substring(1).toInt
                val b = end.toInt

                if (a > b)
                    scold("Invalid IP range: " + addr).^^

                (a to b).map(base + "." + _).toSet
            }
            catch {
                case _: NumberFormatException =>
                    scold("Invalid IP range: " + addr).^^

                    // Never happens.
                    Set.empty
            }
        }
        else
            Set(addr)
    }

    /**
     * Interactively asks for password.
     *
     * @param uname Username.
     * @return Password.
     */
    private def askPassword(uname: String): String = {
        assert(uname != null)

        ask("Password for '" + uname + "': ", "", true)
    }
}

/**
 * Companion object that does initialization of the command.
 */
object VisorDeployCommand {
    addHelp(
        name = "deploy",
        shortInfo = "Copies file or folder to remote host.",
        longInfo = List(
            "Copies file or folder to remote host.",
            "Command relies on SFTP protocol."
        ),
        spec = List(
            "deploy -h={<username>{:<password>}@}<host>{:<port>} {-u=<username>}",
            "    {-p=<password>} {-k=<path>} -s=<path> {-d<path>}"
        ),
        args = List(
            "-h={<username>{:<password>}@}<host>{:<port>}" -> List(
                "Host specification.",
                " ",
                "<host> can be a hostname, IP or range of IPs.",
                "Example of range is 192.168.1.100~150,",
                "which means all IPs from 192.168.1.100 to 192.168.1.150 inclusively.",
                " ",
                "Default port number is 22.",
                " ",
                "This option can be provided multiple times."
            ),
            "-u=<username>" -> List(
                "Default username.",
                "Used if specification doesn't contain username.",
                "If default is not provided as well, current local username will be used."
            ),
            "-p=<password>" -> List(
                "Default password.",
                "Used if specification doesn't contain password.",
                "If default is not provided as well, it will be asked interactively."
            ),
            "-k=<path>" -> List(
                "Path to private key file.",
                "If provided, it will be used for all specifications that doesn't contain password."
            ),
            "-s=<path>" -> "Source path.",
            "-d=<path>" -> List(
                "Destination path (relative to $IGNITE_HOME).",
                "If not provided, files will be copied to the root of $IGNITE_HOME."
            )
        ),
        examples = List(
            "deploy -h=uname:passwd@host -s=/local/path -d=/remote/path" ->
                "Copies file or folder to remote host (password authentication).",
            "deploy -h=uname@host -k=ssh-key.pem -s=/local/path -d=/remote/path" ->
                "Copies file or folder to remote host (private key authentication)."
        ),
        ref = VisorConsoleCommand(cmd.deploy, cmd.deploy)
    )

    /** Singleton command. */
    private val cmd = new VisorDeployCommand

    /**
     * Singleton.
     */
    def apply() = cmd

    /**
     * Implicit converter from visor to commands "pimp".
     *
     * @param vs Visor tagging trait.
     */
    implicit def fromDeploy2Visor(vs: VisorTag): VisorDeployCommand = cmd
}
