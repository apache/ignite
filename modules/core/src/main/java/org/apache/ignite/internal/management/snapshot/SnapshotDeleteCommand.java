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

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotDeleteProcess;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotDeleteProcessResult;
import org.apache.ignite.internal.processors.rollingupgrade.feature.SupportedFeatureRegistry;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Snapshot deletion command.
 *
 * @see SupportedFeatureRegistry#SNAPSHOT_DELETE_FEATURE
 * @see SnapshotDeleteProcess
 */
public class SnapshotDeleteCommand extends AbstractSnapshotCommand<SnapshotDeleteCommandArg, SnapshotDeleteProcessResult> {
    /** */
    public static final String DESC = "Deletes the snapshot and all its incremental snapshots from all online server nodes";

    /** */
    public static final String UNSURED_DELETION_PREF = "WARNING: the following nodes found snapshot data but might not " +
        "remove it completely ";

    /** */
    public static final String REMOVED_PREF = "Snapshot removal is completed on ";

    /** */
    public static final String NODE_NOT_FOUND_PREF = "NOTE: the following nodes can't find any snapshot data, " +
        "operation skipped ";

    /** */
    public static final String NOT_FOUND = "Snapshot not found on available server nodes.";

    /** */
    public static final String MISSING_BASELINES = "WARNING: the snapshot's baseline nodes with the following consistent " +
        "ids are missing in current cluster ";

    /**
     * {@inheritDoc}
     */
    @Override public String description() {
        return DESC;
    }

    /** {@inheritDoc} */
    @Override public Class<SnapshotDeleteCommandArg> argClass() {
        return SnapshotDeleteCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<SnapshotDeleteTask> taskClass() {
        return SnapshotDeleteTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(SnapshotDeleteCommandArg arg, SnapshotDeleteProcessResult res, Consumer<String> printer) {
        boolean found = false;

        if (!res.uncompletedNodes().isEmpty()) {
            found = true;

            printer.accept(UNSURED_DELETION_PREF + nodeIdPairsStrLst(res.uncompletedNodes()));

            printer.accept("");
        }

        if (!res.completedNodes().isEmpty()) {
            found = true;

            printer.accept(REMOVED_PREF + nodeIdPairsStrLst(res.completedNodes()));
            printer.accept("");
        }

        if (found) {
            if (!res.emptyNodes().isEmpty())
                printer.accept(NODE_NOT_FOUND_PREF + nodeIdPairsStrLst(res.emptyNodes()));

            if (!res.absentBaselines().isEmpty())
                printer.accept(MISSING_BASELINES + nodeIdsStrLst(res.absentBaselines()));
        }
        else {
            assert !res.emptyNodes().isEmpty();

            printer.accept(NOT_FOUND);
        }
    }

    /** */
    private static String nodeIdPairsStrLst(Map<UUID, String> uuids) {
        return "[cnt=" + uuids.size() + "]: " + uuids.entrySet().stream()
            .map(e -> e.getValue() + " [uuid=" + e.getKey() + ']')
            .collect(Collectors.joining(", "));
    }

    /** */
    private static String nodeIdsStrLst(Collection<String> uuids) {
        return "[cnt=" + uuids.size() + "]: " + String.join(", ", uuids);
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt(SnapshotDeleteCommandArg arg) {
        return "This operation will completely remove snapshot: '" + arg.snapshotName() + "' and all its incrementals." +
            U.nl() + U.nl() +
            "If the security is enabled, the operation requires the snapshot administration permissions." +
            U.nl() + U.nl() +
            "Deletion in not snapshots Ignite's directories and deletion of any data without or corrupted snapshot " +
                "metadata are prohibited." +
            U.nl() + U.nl() +
            "The operation cannot be reverted." +
            U.nl() + U.nl() +
            "NOTE: Snapshot data on offline server nodes will remain untouched.";
    }
}
