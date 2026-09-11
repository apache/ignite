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
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotDeleteProcess;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotDeleteProcessResult;
import org.apache.ignite.internal.processors.rollingupgrade.feature.SupportedFeatureRegistry;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Snapshot deletion command.
 *
 * @see SupportedFeatureRegistry#SNAPSHOT_DELETE_FEATURE
 * @see SnapshotDeleteProcess
 */
public class SnapshotDeleteCommand extends AbstractSnapshotCommand<SnapshotDeleteCommandArg, SnapshotDeleteProcessResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Deletes snapshot and all its increments from all the online server nodes";
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

        if (!F.isEmpty(res.uncompletedNodes())) {
            found = true;

            printer.accept("WARNING, the following nodes found snapshot data but might not remove it completely "
                + nodeIdsStrLst(res.uncompletedNodes()));

            printer.accept("");
        }

        if (!F.isEmpty(res.completedNodes())) {
            found = true;

            printer.accept("Snapshot removed on the following nodes " + nodeIdsStrLst(res.completedNodes()));
            printer.accept("");
        }

        if (found) {
            if (!F.isEmpty(res.emptyNodes())) {
                printer.accept("NOTE, the following nodes didn't find any snapshot data, nothing to delete "
                    + nodeIdsStrLst(res.emptyNodes()));
            }
        }
        else {
            if (!F.isEmpty(res.emptyNodes()))
                printer.accept("Snapshot not found on current server nodes.");
            else
                printer.accept("Unknown result.");
        }
    }

    /** */
    private static String nodeIdsStrLst(Collection<UUID> uuids) {
        return "[cnt=" + uuids.size() + "]: " + uuids.stream().map(UUID::toString).collect(Collectors.joining(", "));
    }

    /** {@inheritDoc} */
    @Override public String confirmationPrompt(SnapshotDeleteCommandArg arg) {
        return "This will delete snapshot '" + arg.snapshotName() +
            "' and all its increments from all online server nodes." +
            U.nl() + U.nl() +
            "WARNING: the snapshot integrity, topology and correctness aren't checked." +
            " Snapshot daya on offline server nodes aren't deleted." +
            U.nl() + U.nl() +
            "The operation is irreversible.";
    }
}
