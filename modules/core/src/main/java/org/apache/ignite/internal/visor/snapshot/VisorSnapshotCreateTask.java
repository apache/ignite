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
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMXBeanImpl;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * @see IgniteSnapshotManager#createSnapshot(String)
 * @see IgniteSnapshotManager#cancelSnapshot(String)
 * @see IgniteSnapshotManager#isSnapshotCreating()
 */
@GridInternal
public class VisorSnapshotCreateTask extends VisorOneNodeTask<VisorSnapshotCreateTaskArgs, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob job(VisorSnapshotCreateTaskArgs arg) {
        switch (arg.jobAction()) {
            case START:
                return new VisorSnapshotCreateJob(arg.snapshotName(), debug);

            case CANCEL:
                return new VisorSnapshotCancelJob(arg.snapshotName(), debug);

            case STATUS:
                return new VisorSnapshotStatuslJob(null, debug);

            default:
                throw new IgniteException("Unexpected VisorSnapshotTaskAction: " + arg.jobAction());
        }
    }

    /** */
    private static class VisorSnapshotCreateJob extends VisorJob<String, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param name Snapshot name.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotCreateJob(String name, boolean debug) {
            super(name, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(String name) throws IgniteException {
            new SnapshotMXBeanImpl(ignite.context()).createSnapshot(name);

            return "Snapshot operation started: " + name;
        }
    }

    /** */
    private static class VisorSnapshotCancelJob extends VisorJob<String, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param name Snapshot name.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotCancelJob(String name, boolean debug) {
            super(name, debug);
        }

        /** {@inheritDoc} */
        @Override protected @Nullable String run(String name) throws IgniteException {
            ignite.snapshot().cancelSnapshot(name).get();

            return "Snapshot is cancel.";
        }
    }

    /** */
    private static class VisorSnapshotStatuslJob extends VisorJob<String, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Void arg.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotStatuslJob(@Nullable String arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(@Nullable String arg) throws IgniteException {
            ((IgniteSnapshotManager)ignite.snapshot()).isSnapshotCreating();

            return "Snapshot create operation is: " +
                (((IgniteSnapshotManager)ignite.snapshot()).isSnapshotCreating() ? "running." : "absent.");
        }
    }
}
