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

package org.apache.ignite.internal.management.wal;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/** */
public class WalSetStateResultReporter {
    /**
     * Print WAL disable/enable command result.
     *
     * @param arg Command argument.
     * @param res Command result.
     * @param printer Output consumer.
     */
    public static void printResult(WalStateCommandArg arg, WalSetStateTaskResult res, Consumer<String> printer) {
        String operation = arg instanceof WalEnableCommand.WalEnableCommandArg ? "enable" : "disable";
        List<String> successGrps = res.successGroups();
        Map<String, String> errors = res.errorsByGroup();

        if (!successGrps.isEmpty()) {
            printer.accept("Successfully " + operation + "d WAL for groups:");
            for (String grp : successGrps)
                printer.accept("  " + grp);
        }

        if (errors != null && !errors.isEmpty()) {
            printer.accept("Failed to " + operation + " WAL for groups:");
            for (Map.Entry<String, String> entry : errors.entrySet())
                printer.accept("  " + entry.getKey() + " - " + entry.getValue());
        }
    }

    /** Default constructor. */
    private WalSetStateResultReporter() {
        // No-op.
    }
}
