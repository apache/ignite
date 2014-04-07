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
 * Contains Visor command `license` implementation.
 *
 * ==Help==
 * {{{
 * +---------------------------------------------------------------------------+
 * | license | Shows information about all licenses that are used on the grid. |
 * |         | Also can be used to update on of the licenses.                  |
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
package object license
