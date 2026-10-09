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
    public static final String HEADER = "Snapshots found on the following nodes:";

    /** */
    public static final String DESC = "Lists all snapshots on all online server nodes with their sizes";

    /** */
    public static final String NO_SNAPSHOTS = "No snapshots found.";

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

        for (int nodeIdx = 0; nodeIdx < res.nodesIds().length; nodeIdx++) {
            // Skip line before node.
            printer.accept("");

            printer.accept("\tNode '%s' [uuid=%s]:".formatted(res.consistentIds()[nodeIdx], res.nodesIds()[nodeIdx]));

            SnapshotListJobResult nodeResult = res.nodesSnapshots()[nodeIdx];

            if (nodeResult.snapshots().isEmpty()) {
                printer.accept("\t\t" + NO_SNAPSHOTS);

                continue;
            }

            nodeResult.snapshots().forEach((snpName, snpInfo) -> {
                printer.accept("\t\tSnapshot '%s': totalSize=%s (%db), created='%s' (epoch=%d)".formatted(
                    snpName,
                    U.humanReadableByteCount(snpInfo.size()),
                    snpInfo.size(),
                    DATE_FORMATTER.format(Instant.ofEpochMilli(snpInfo.date())),
                    snpInfo.date()
                ));

                SnapshotListJobResult.SnapshotInfo extStors = snpInfo.externalStorages();
                SnapshotListJobResult.SnapshotInfo incs = snpInfo.incrementals();

                if (extStors != null) {
                    printer.accept("\t\t\texternal storages: cnt=%d, size=%s (%db)".formatted(
                        extStors.number(),
                        U.humanReadableByteCount(extStors.size()),
                        extStors.size()
                    ));
                }

                if (incs != null) {
                    printer.accept("\t\t\tincremental snapshots: cnt=%d, size=%s (%db), modified='%s' (epoch=%d)".formatted(
                        incs.number(),
                        U.humanReadableByteCount(incs.size()),
                        incs.size(),
                        DATE_FORMATTER.format(Instant.ofEpochMilli(incs.date())),
                        incs.date()
                    ));
                }
            });
        }

        // Drop a line.
        printer.accept("");
    }
}
