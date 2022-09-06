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

package org.apache.ignite.internal.visor.snapshot;

import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.visor.VisorJob;

/**
 * @see IgniteSnapshot#createSnapshot(String)
 */
@GridInternal
public class VisorSnapshotCreateTask extends VisorSnapshotOneNodeTask<VisorSnapshotCreateTaskArg, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorSnapshotCreateTaskArg, String> job(VisorSnapshotCreateTaskArg arg) {
        return new VisorSnapshotCreateJob(arg, debug);
    }

    /** */
    private static class VisorSnapshotCreateJob extends VisorJob<VisorSnapshotCreateTaskArg, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Snapshot create task argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotCreateJob(VisorSnapshotCreateTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(VisorSnapshotCreateTaskArg arg) throws IgniteException {
            IgniteFutureImpl<Void> fut =
                ignite.context().cache().context().snapshotMgr().createSnapshot(arg.snapshotName(), arg.snapshotPath());

            IgniteSnapshotManager.ClusterSnapshotFuture snpFut = (IgniteSnapshotManager.ClusterSnapshotFuture)fut.internalFuture();

            if (arg.sync() || fut.isDone())
                fut.get();

            String msgOperId = snpFut.requestId() != null ? ", operId=" + snpFut.requestId() : "";

            return "Snapshot create operation " + (arg.sync() ? "completed successfully" : "started") +
                " [name=" + arg.snapshotName() + msgOperId + ']';
        }
    }
}
