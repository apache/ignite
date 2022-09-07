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

import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMXBeanImpl;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;

/**
 * @see IgniteSnapshotManager#cancelSnapshotOperation(UUID)
 */
@GridInternal
public class VisorSnapshotCancelTask extends VisorSnapshotOneNodeTask<VisorSnapshotCancelTaskArg, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorSnapshotCancelTaskArg, String> job(VisorSnapshotCancelTaskArg arg) {
        return new VisorSnapshotCancelJob(arg, debug);
    }

    /** */
    private static class VisorSnapshotCancelJob extends VisorJob<VisorSnapshotCancelTaskArg, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param taskArg Task argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotCancelJob(VisorSnapshotCancelTaskArg taskArg, boolean debug) {
            super(taskArg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(VisorSnapshotCancelTaskArg taskArg) throws IgniteException {
            if (taskArg.requestId() != null) {
                new SnapshotMXBeanImpl(ignite.context()).cancelSnapshotOperation(taskArg.requestId().toString());

                return "Snapshot operation cancelled [id=" + taskArg.requestId() + "].";
            }

            new SnapshotMXBeanImpl(ignite.context()).cancelSnapshot(taskArg.snapshotName());

            return "Snapshot operation cancelled [snapshot=" + taskArg.snapshotName() + "].";
        }
    }
}
