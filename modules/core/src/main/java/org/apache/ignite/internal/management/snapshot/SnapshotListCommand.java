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

package org.apache.ignite.internal.management.snapshot;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.function.Consumer;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListJobResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListTaskResult;
import org.apache.ignite.internal.util.typedef.internal.U;

/** Snapshot list command. */
public class SnapshotListCommand extends AbstractSnapshotCommand<SnapshotListCommandArg, SnapshotListTaskResult> {
    /** */
    public static final String HEADER = "Snapshots lists on the following nodes:";

    /** */
    public static final String DESC = "Lists all snapshots on all online server nodes with their sizes";

    /** */
    public static final String NO_SNAPSHOTS = "No snapshots found.";

    /** */
    public static final String NODE_PREF = "Node ";

    /** */
    private static final String PATTERN_FORMAT = "yyyy-MM-dd HH:mm:ss Z";

    /** */
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(PATTERN_FORMAT).withZone(ZoneId.systemDefault());

    /** {@inheritDoc} */
    @Override public String description() {
        return DESC;
    }

    /** {@inheritDoc} */
    @Override public Class<SnapshotListCommandArg> argClass() {
        return SnapshotListCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<SnapshotListTask> taskClass() {
        return SnapshotListTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(SnapshotListCommandArg arg, SnapshotListTaskResult res, Consumer<String> printer) {
        printer.accept(HEADER);

        for (int n = 0; n < res.nodesIds().length; n++) {
            // Skip line before node.
            printer.accept("");

            printer.accept("\tNode '%s' [uuid=%s]:".formatted(res.consistentIds()[n], res.nodesIds()[n]));

            SnapshotListJobResult nodeSnps = res.snapshots()[n];

            if (nodeSnps.snapshotNames().length == 0) {
                printer.accept("\t\t" + NO_SNAPSHOTS);

                continue;
            }

            for (int snpIdx = 0; snpIdx < nodeSnps.snapshotNames().length; snpIdx++) {
                String name = nodeSnps.snapshotNames()[snpIdx];
                long size = nodeSnps.sizes()[snpIdx];
                long dateLong = nodeSnps.creationTimes()[snpIdx];

                printer.accept("\t\tSnapshot '%s': size=%s (%db), created='%s' (epoch=%d)".formatted(
                    name,
                    U.humanReadableByteCount(size),
                    size,
                    DATE_FORMATTER.format(Instant.ofEpochMilli(dateLong)),
                    dateLong
                ));

                // Also incremental snapshots exist.
                if (nodeSnps.incrementalsCount()[snpIdx] == 0)
                    continue;

                // Also incremental snapshots exist.
                int cnt = nodeSnps.incrementalsCount()[snpIdx];
                size = nodeSnps.incrementalsSizes()[snpIdx];
                dateLong = nodeSnps.creationTimes()[snpIdx];

                printer.accept("\t\t\tincremental snapshots: cnt=%d, size=%s (%db), modified='%s' (epoch=%d)".formatted(
                    cnt,
                    U.humanReadableByteCount(size),
                    size,
                    DATE_FORMATTER.format(Instant.ofEpochMilli(dateLong)),
                    dateLong
                ));
            }
        }

        // Drop a line.
        printer.accept("");
    }
}
