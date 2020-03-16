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

package org.apache.ignite.internal.commandline.query;

import org.apache.ignite.mxbean.ComputeMXBean;
import org.apache.ignite.mxbean.QueryMXBean;
import org.apache.ignite.mxbean.ServiceMXBean;
import org.apache.ignite.mxbean.TransactionsMXBean;
import org.jetbrains.annotations.Nullable;

/**
 * Subcommands of the kill command.
 *
 * @see KillCommand
 * @see QueryMXBean
 * @see ComputeMXBean
 * @see TransactionsMXBean
 * @see ServiceMXBean
 */
public enum KillQuerySubcommand {
    /** Kill scan query. */
    SCAN_QUERY("scan"),

    /** Kill continuous query. */
    CONTINUOUS_QUERY("continuous"),

    /** Kill sql query. */
    SQL_QUERY("sql"),

    /** Kill compute task. */
    COMPUTE("compute"),

    /** Kill transaction. */
    TRANSACTION("tx"),

    /** Kill service. */
    SERVICE("service");

    /** Subcommand name. */
    private final String name;

    /** @param name Encryption subcommand name. */
    KillQuerySubcommand(String name) {
        this.name = name;
    }

    /**
     * @param text Command text (case insensitive).
     * @return Command for the text. {@code Null} if there is no such command.
     */
    @Nullable public static KillQuerySubcommand of(String text) {
        for (KillQuerySubcommand cmd : KillQuerySubcommand.values()) {
            if (cmd.name.equalsIgnoreCase(text))
                return cmd;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
