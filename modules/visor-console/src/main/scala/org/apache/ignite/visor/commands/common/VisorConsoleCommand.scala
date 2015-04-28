/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.visor.commands.common

import org.apache.ignite.visor.visor.NA

import scala.collection.JavaConversions._

/**
 * Command implementation.
 */
trait VisorConsoleCommand {
    protected def name: String

    /**
     * Prints properly formatted error message like:
     * {{{
     * (wrn) <visor>: warning message
     * }}}
     *
     * @param warnMsgs Error messages to print. If `null` - this function is no-op.
     */
    protected def warn(warnMsgs: Any*) {
        assert(warnMsgs != null)

        warnMsgs.foreach{
            case ex: Throwable => println(s"(wrn) <visor>: ${ex.getMessage}")
            case line => println(s"(wrn) <visor>: $line")
        }
    }

    /**
     * Prints standard 'not connected' warn message.
     */
    protected def adviseToConnect() {
        warn(
            "Visor is disconnected.",
            "Type 'open' to connect Visor console or 'help open' to get help."
        )
    }

    /**
     * Prints warn message and advise.
     *
     * @param warnMsgs Warning messages.
     */
    protected def scold(warnMsgs: Any*) {
        assert(warnMsgs != null)

        warn(warnMsgs: _*)
        warn(s"Type 'help $name' to see how to use this command.")
    }

    /**
     * Joins array of strings to a single string with line feed.
     *
     * @param lines Lines to join together.
     * @return Joined line.
     */
    protected def join(lines: java.lang.Iterable[_ <: Any]) = {
        if (lines == null || lines.isEmpty)
            NA
        else
            lines.mkString("[", ", ", "]")
    }

    protected def join(lines: Array[_ <: Any]) = {
        if (lines == null || lines.isEmpty)
            NA
        else
            lines.mkString("[", ", ", "]")
    }
}
