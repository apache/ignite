/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.management.snapshot;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotDeleteProcess;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotDeleteProcessResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.resources.IgniteInstanceResource;

import static org.apache.ignite.internal.processors.rollingupgrade.feature.SupportedFeatureRegistry.SNAPSHOT_DELETE_FEATURE;

/**
 * @see IgniteSnapshotManager#deleteSnapshot(String, String)
 * @see SnapshotDeleteProcess
 */
@GridInternal
public class SnapshotDeleteTask extends VisorOneNodeTask<SnapshotDeleteCommandArg, SnapshotDeleteProcessResult> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** */
    @IgniteInstanceResource
    private IgniteEx ignite;

    /** {@inheritDoc} */
    @Override protected VisorJob<SnapshotDeleteCommandArg, SnapshotDeleteProcessResult> job(SnapshotDeleteCommandArg arg) {
        return new SnapshotDeleteJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<SnapshotDeleteCommandArg> arg) {
        return super.jobNodes(arg);
    }

    /** {@inheritDoc} */
    @Override protected Map<? extends ComputeJob, ClusterNode> map0(
        List<ClusterNode> subgrid,
        VisorTaskArgument<SnapshotDeleteCommandArg> arg
    ) {
        if (!ignite.context().rollingUpgrade().features().isActive(SNAPSHOT_DELETE_FEATURE))
            throw new IgniteException(SnapshotDeleteProcess.OP_REJECT_FEATURE_MSG);

        return super.map0(subgrid, arg);
    }

    /** */
    private static class SnapshotDeleteJob extends SnapshotJob<SnapshotDeleteCommandArg, SnapshotDeleteProcessResult> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Snapshot delete task argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected SnapshotDeleteJob(SnapshotDeleteCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected SnapshotDeleteProcessResult run(SnapshotDeleteCommandArg arg) {
            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

            return snpMgr.deleteSnapshot(arg.snapshotName(), arg.src()).get();
        }
    }
}
