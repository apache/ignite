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

        for (int nodeIdx = 0; nodeIdx < res.nodesIds().length; nodeIdx++) {
            // Skip line before node.
            printer.accept("");

            printer.accept("\tNode '%s' [uuid=%s]:".formatted(res.consistentIds()[nodeIdx], res.nodesIds()[nodeIdx]));

            SnapshotListJobResult nodeSnps = res.snapshots()[nodeIdx];

            if (nodeSnps.snapshots().length == 0) {
                printer.accept("\t\t" + NO_SNAPSHOTS);

                continue;
            }

            // Optional info of all the snapshot external storages.
            SnapshotListJobResult.SnapshotInfo[] allExtStors = nodeSnps.externalStorages();
            // Optional info of all the incrementsl snapshots.
            SnapshotListJobResult.SnapshotInfo[] allIncSnps = nodeSnps.incrementalSnapshots();

            for (int snpIdx = 0; snpIdx < nodeSnps.snapshots().length; snpIdx++) {
                SnapshotListJobResult.SnapshotInfo snp = nodeSnps.snapshots()[snpIdx];

                printer.accept("\t\tSnapshot '%s': totalSize=%s (%db), created='%s' (epoch=%d)".formatted(
                    snp.name(),
                    U.humanReadableByteCount(snp.size()),
                    snp.size(),
                    DATE_FORMATTER.format(Instant.ofEpochMilli(snp.date())),
                    snp.date()
                ));

                // Optional certain snapshot external storages' info.
                SnapshotListJobResult.SnapshotInfo snpExtStors = allExtStors == null || allExtStors[snpIdx] == null
                    ? null
                    : allExtStors[snpIdx];

                if (snpExtStors != null) {
                    printer.accept("\t\t\texternal storages: cnt=%d, size=%s (%db)".formatted(
                        snpExtStors.number(),
                        U.humanReadableByteCount(snpExtStors.size()),
                        snpExtStors.size()
                    ));
                }

                // Optional certain snapshot incrementals parts' info.
                SnapshotListJobResult.SnapshotInfo snpIncsParts = allIncSnps == null || allIncSnps[snpIdx] == null
                    ? null
                    : allIncSnps[snpIdx];

                if (snpIncsParts == null)
                    continue;

                assert snpIncsParts.date() != null;

                printer.accept("\t\t\tincremental snapshots: cnt=%d, size=%s (%db), modified='%s' (epoch=%d)".formatted(
                    snpIncsParts.number(),
                    U.humanReadableByteCount(snpIncsParts.size()),
                    snpIncsParts.size(),
                    DATE_FORMATTER.format(Instant.ofEpochMilli(snpIncsParts.date())),
                    snpIncsParts.date()
                ));
            }
        }

        // Drop a line.
        printer.accept("");
    }
}
