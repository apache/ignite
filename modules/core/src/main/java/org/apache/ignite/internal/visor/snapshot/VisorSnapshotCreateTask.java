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
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMXBeanImpl;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * @see IgniteSnapshot#createSnapshot(String)
 */
@GridInternal
public class VisorSnapshotCreateTask extends VisorOneNodeTask<VisorSnapshotCreateTaskArgs, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorSnapshotCreateTaskArgs, String> job(VisorSnapshotCreateTaskArgs arg) {
        return new VisorSnapshotCreateJob(arg, debug);
    }

    /** */
    private static class VisorSnapshotCreateJob extends VisorJob<VisorSnapshotCreateTaskArgs, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg VisorSnapshotCreateTaskArgs.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotCreateJob(VisorSnapshotCreateTaskArgs arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(VisorSnapshotCreateTaskArgs arg) throws IgniteException {
            new SnapshotMXBeanImpl(ignite.context()).createSnapshot(arg.snapshotName());

            return "Snapshot operation started: " + arg.snapshotName();
        }
    }

    /** */
    private static class VisorSnapshotCancelJob extends VisorJob<VisorSnapshotCreateTaskArgs, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param args VisorSnapshotCreateTaskArgs.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotCancelJob(VisorSnapshotCreateTaskArgs args, boolean debug) {
            super(args, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(VisorSnapshotCreateTaskArgs args) throws IgniteException {
            ignite.snapshot().cancelSnapshot(args.snapshotName()).get();

            return "Snapshot operation cancelled.";
        }
    }

    /** */
    private static class VisorSnapshotStatuslJob extends VisorJob<VisorSnapshotCreateTaskArgs, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg VisorSnapshotCreateTaskArgs.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotStatuslJob(VisorSnapshotCreateTaskArgs arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(VisorSnapshotCreateTaskArgs arg) throws IgniteException {
            ((IgniteSnapshotManager)ignite.snapshot()).isSnapshotCreating();

            return "Snapshot create operation is: " +
                (((IgniteSnapshotManager)ignite.snapshot()).isSnapshotCreating() ? "running." : "absent.");
        }
    }
}
