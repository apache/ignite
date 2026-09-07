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

import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * @see IgniteSnapshotManager#deleteSnapshot(String, String)
 */
@GridInternal
public class SnapshotDeleteTask extends VisorOneNodeTask<SnapshotDeleteCommandArg, Void> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<SnapshotDeleteCommandArg, Void> job(SnapshotDeleteCommandArg arg) {
        return new SnapshotDeleteJob(arg, debug);
    }

    /** */
    private static class SnapshotDeleteJob extends SnapshotJob<SnapshotDeleteCommandArg, Void> {
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
        @Override protected Void run(SnapshotDeleteCommandArg arg) {
            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

            snpMgr.deleteSnapshot(arg.snapshotName(), arg.dest()).get();

            return null;
        }
    }
}
