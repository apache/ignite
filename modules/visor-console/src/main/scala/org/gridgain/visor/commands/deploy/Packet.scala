/* @scala.file.header */

/*
 * ___    _________________________ ________
 * __ |  / /____  _/__  ___/__  __ \___  __ \
 * __ | / /  __  /  _____ \ _  / / /__  /_/ /
 * __ |/ /  __/ /   ____/ / / /_/ / _  _, _/
 * _____/   /___/   /____/  \____/  /_/ |_|
 *
 */

package org.gridgain.visor.commands

/**
 * ==Overview==
 * Visor 'deploy' command implementation.
 *
 * ==Importing==
 * When using this command from Scala code (not from REPL) you need to make sure to properly
 * import all necessary typed and implicit conversions:
 * <pre name="code" class="scala">
 * import org.gridgain.visor._
 * import commands.deploy.VisorDeployCommand._
 * </pre>
 * Note that `VisorDeployCommand` object contains necessary implicit conversions so that
 * this command would be available via `visor` keyword.
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
 *     visor deploy "-h={<username>{:<password>}@}<host>{:<port>} {-u=<username>}
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
 *         Destination path (relative to GRIDGAIN_HOME).
 *         If not provided, files will be copied to the root of GRIDGAIN_HOME.
 * }}}
 *
 * ====Examples====
 * {{{
 *     visor deploy "-h=uname:passwd@host -s=/local/path -d=/remote/path"
 *         Copies file or directory to remote host (password authentication).
 *     visor deploy "-h=uname@host -k=ssh-key.pem -s=/local/path -d=/remote/path"
 *         Copies file or directory to remote host (private key authentication).
 * }}}
 */
package object deploy
