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

import java.sql.Date;
import java.time.Instant;
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

    /**
     * {@inheritDoc}
     */
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

            if(nodeSnps.snapshotNames().length==0) {
                printer.accept("\t\t" + NO_SNAPSHOTS);

                continue;
            }

            for (int s = 0; s < nodeSnps.snapshotNames().length; s++) {
                String name = nodeSnps.snapshotNames()[s];
                long size = nodeSnps.sizes()[s];
                long epochTime = nodeSnps.creationTimes()[s];

                printer.accept("\t\tSnapshot '%s' [size=%s (%db), created='%s' (epochSeconds=%d)]".formatted(
                    name,
                    U.humanReadableByteCount(size),
                    size,
                    Date.from(Instant.ofEpochSecond(epochTime)).toString(),
                    epochTime
                ));
            }
        }

        // Drop a line.
        printer.accept("");
    }
}
