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

package org.apache.ignite.visor.commands

import org.apache.ignite.internal.util.scala.impl
import org.apache.ignite.visor.visor

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
