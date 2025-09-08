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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;

/**
 * Snapshot restore operation handling task.
 */
public class SnapshotHandlerRestoreTask {
    /** */
    private final IgniteEx ignite;

    /** */
    private final IgniteLogger log;

    /** */
    private final SnapshotHandlerRestoreJob job;

    /** */
    SnapshotHandlerRestoreTask(
        IgniteEx ignite,
        IgniteLogger log,
        SnapshotFileTree sft,
        Collection<String> grps,
        boolean check
    ) {
        job = new SnapshotHandlerRestoreJob(ignite, sft, grps, check);
        this.ignite = ignite;
        this.log = log;
    }

    /** */
    public Map<String, SnapshotHandlerResult<Object>> execute() {
        return job.execute0();
    }

    /** */
    public void reduce(
        String snapshotName,
        Map<ClusterNode, Map<Object, Map<String, SnapshotHandlerResult<?>>>> results
    ) {
        Map<String, List<SnapshotHandlerResult<?>>> clusterResults = new HashMap<>();
        Collection<UUID> execNodes = new ArrayList<>(results.size());

        // Checking node -> Map by consistend id.
        for (Map.Entry<ClusterNode, Map<Object, Map<String, SnapshotHandlerResult<?>>>> nodeRes : results.entrySet()) {
            // Consistent id -> Map by handler name.
            for (Map.Entry<Object, Map<String, SnapshotHandlerResult<?>>> res : nodeRes.getValue().entrySet()) {
                // Depending on the job mapping, we can get several different results from one node.
                execNodes.add(nodeRes.getKey().id());

                Map<String, SnapshotHandlerResult<?>> nodeDataMap = res.getValue();

                assert nodeDataMap != null : "At least the default snapshot restore handler should have been executed ";

                for (Map.Entry<String, SnapshotHandlerResult<?>> entry : nodeDataMap.entrySet()) {
                    String hndName = entry.getKey();

                    clusterResults.computeIfAbsent(hndName, v -> new ArrayList<>()).add(entry.getValue());
                }
            }
        }

        try {
            ignite.context().cache().context().snapshotMgr().handlers().completeAll(
                SnapshotHandlerType.RESTORE, snapshotName, clusterResults, execNodes, wrns -> {});
        }
        catch (Exception e) {
            log.warning("The snapshot operation will be aborted due to a handler error [snapshot=" + snapshotName + "].", e);

            throw new IgniteException(e);
        }
    }

    /** Invokes all {@link SnapshotHandlerType#RESTORE} handlers locally. */
    private static class SnapshotHandlerRestoreJob {
        /** */
        private final IgniteEx ignite;

        /** */
        private final SnapshotFileTree sft;

        /** */
        private final Collection<String> rqGrps;

        /** */
        private final boolean check;

        /**
         * @param grps Cache group names.
         * @param check If {@code true} check snapshot before restore.
         */
        SnapshotHandlerRestoreJob(
            IgniteEx ignite,
            SnapshotFileTree sft,
            Collection<String> grps,
            boolean check
        ) {
            this.ignite = ignite;
            this.sft = sft;
            this.rqGrps = grps;
            this.check = check;
        }

        /** */
        public Map<String, SnapshotHandlerResult<Object>> execute0() {
            try {
                IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();
                SnapshotMetadata meta = snpMgr.readSnapshotMetadata(sft.meta());

                return snpMgr.handlers().invokeAll(SnapshotHandlerType.RESTORE,
                    new SnapshotHandlerContext(meta, rqGrps, ignite.localNode(), sft, false, check, null, null));
            }
            catch (IgniteCheckedException | IOException e) {
                throw new IgniteException(e);
            }
        }
    }
}
