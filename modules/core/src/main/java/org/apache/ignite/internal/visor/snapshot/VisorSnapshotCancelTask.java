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
public class VisorSnapshotCancelTask extends VisorSnapshotOneNodeTask<UUID, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<UUID, String> job(UUID arg) {
        return new VisorSnapshotCancelJob(arg, debug);
    }

    /** */
    private static class VisorSnapshotCancelJob extends VisorJob<UUID, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param operId Snapshot operation ID.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotCancelJob(UUID operId, boolean debug) {
            super(operId, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(UUID operId) throws IgniteException {
            new SnapshotMXBeanImpl(ignite.context()).cancelSnapshotOperation(operId.toString());

            return "Snapshot operation cancelled [operId=" + operId + "].";
        }
    }
}
