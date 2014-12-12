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

import org.gridgain.grid.util.scala.impl

import org.gridgain.visor.visor

/**
 * Command implementation.
 */
trait VisorConsoleCommand {
    /**
     * Command without arguments.
     */
    def invoke()

    /**
     * Command with arguments.
     *
     * @param args - arguments as string.
     */
    def invoke(args: String)
}

/**
 * Singleton companion object.
 */
object VisorConsoleCommand {
    /**
     * Create `VisorConsoleCommand`.
     *
     * @param emptyArgs Function to execute in case of no args passed to command.
     * @param withArgs Function to execute in case of some args passed to command.
     * @return New instance of `VisorConsoleCommand`.
     */
    def apply(emptyArgs: () => Unit, withArgs: (String) => Unit) = {
        new VisorConsoleCommand {
            @impl def invoke() = emptyArgs.apply()

            @impl def invoke(args: String) = withArgs.apply(args)
        }
    }

    /**
     * Create `VisorConsoleCommand`.
     *
     * @param emptyArgs Function to execute in case of no args passed to command.
     * @return New instance of `VisorConsoleCommand`.
     */
    def apply(emptyArgs: () => Unit) = {
        new VisorConsoleCommand {
            @impl def invoke() = emptyArgs.apply()

            @impl def invoke(args: String) {
                visor.warn(
                    "Invalid arguments for command without arguments.",
                    "Type 'help' to print commands list."
                )
            }
        }
    }
}
