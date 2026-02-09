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
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Result of WAL enable/disable operation.
 */
public class WalSetStateTaskResult extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Successfully processed groups. */
    List<String> successGrps;

    /** Errors by group name. */
    Map<String, String> errorsByGrp;

    /** Default constructor. */
    public WalSetStateTaskResult() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param successGrps Successfully processed groups.
     * @param errorsByGrp Error messages.
     */
    public WalSetStateTaskResult(List<String> successGrps, Map<String, String> errorsByGrp) {
        this.successGrps = successGrps;
        this.errorsByGrp = errorsByGrp;
    }

    /**
     * Print WAL disable/enable command result.
     *
     * @param enable If {@code true} then "enable" operation, otherwise "disable".
     * @param printer Output consumer.
     */
    void print(boolean enable, Consumer<String> printer) {
        String op = enable ? "enable" : "disable";

        if (!successGrps.isEmpty()) {
            printer.accept("Successfully " + op + "d WAL for groups:");

            for (String grp : successGrps)
                printer.accept("  " + grp);
        }

        if (!F.isEmpty(errorsByGrp)) {
            printer.accept("Failed to " + op + " WAL for groups:");

            for (Map.Entry<String, String> entry : errorsByGrp.entrySet())
                printer.accept("  " + entry.getKey() + " - " + entry.getValue());
        }
    }
}
