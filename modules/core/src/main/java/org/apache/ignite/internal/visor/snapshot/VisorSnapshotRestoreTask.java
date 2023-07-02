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

import java.util.Arrays;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.management.snapshot.SnapshotRestoreCommandArg;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.visor.VisorJob;

/**
 * Visor snapshot restore task.
 */
@GridInternal
public class VisorSnapshotRestoreTask extends VisorSnapshotOneNodeTask<SnapshotRestoreCommandArg, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<SnapshotRestoreCommandArg, String> job(SnapshotRestoreCommandArg arg) {
        if (arg.cancel())
            return new VisorSnapshotRestoreCancelJob(arg, debug);
        else if (arg.status())
            return new VisorSnapshotRestoreStatusJob(arg, debug);

        return new VisorSnapshotStartRestoreJob(arg, debug);
    }

    /** */
    private static class VisorSnapshotStartRestoreJob extends VisorSnapshotJob<SnapshotRestoreCommandArg, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Restore task argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotStartRestoreJob(SnapshotRestoreCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(SnapshotRestoreCommandArg arg) throws IgniteException {
            IgniteFutureImpl<Void> fut = ignite.context().cache().context().snapshotMgr().restoreSnapshot(
                arg.snapshotName(),
                arg.src(),
                arg.groups() == null ? null : Arrays.asList(arg.groups()),
                arg.increment(),
                arg.check()
            );

            IgniteSnapshotManager.ClusterSnapshotFuture snpFut =
                fut.internalFuture() instanceof IgniteSnapshotManager.ClusterSnapshotFuture ?
                    (IgniteSnapshotManager.ClusterSnapshotFuture)fut.internalFuture() : null;

            if (arg.sync() || fut.isDone())
                fut.get();

            String msgSuff = arg.sync() ? "completed successfully" : "started";
            String msgGrps = arg.groups() == null ? "" : ", group(s)=" + String.join(",", arg.groups());
            String msgId = snpFut != null && snpFut.requestId() != null ? ", id=" + snpFut.requestId() : "";

            return "Snapshot cache group restore operation " + msgSuff +
                " [name=" + arg.snapshotName() + msgGrps + msgId + ']';
        }
    }

    /**
     * @deprecated Use {@link VisorSnapshotCancelTask} instead.
     */
    @Deprecated
    private static class VisorSnapshotRestoreCancelJob extends VisorSnapshotJob<SnapshotRestoreCommandArg, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Restore task argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotRestoreCancelJob(SnapshotRestoreCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(SnapshotRestoreCommandArg arg) throws IgniteException {
            boolean stopped = ignite.snapshot().cancelSnapshotRestore(arg.snapshotName()).get();

            return "Snapshot cache group restore operation " +
                (stopped ? "canceled" : "is NOT running") + " [snapshot=" + arg.snapshotName() + ']';
        }
    }

    /**
     * @deprecated Use {@link VisorSnapshotStatusTask} instead.
     */
    @Deprecated
    private static class VisorSnapshotRestoreStatusJob extends VisorSnapshotJob<SnapshotRestoreCommandArg, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Restore task argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotRestoreStatusJob(SnapshotRestoreCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(SnapshotRestoreCommandArg arg) throws IgniteException {
            boolean state = ignite.context().cache().context().snapshotMgr().restoreStatus(arg.snapshotName()).get();

            return "Snapshot cache group restore operation is " + (state ? "" : "NOT ") +
                "running [snapshot=" + arg.snapshotName() + ']';
        }
    }
}
