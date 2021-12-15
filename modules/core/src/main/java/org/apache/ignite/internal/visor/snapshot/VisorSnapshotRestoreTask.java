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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreStatusDetails;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Visor snapshot restore task.
 */
@GridInternal
public class VisorSnapshotRestoreTask extends VisorOneNodeTask<VisorSnapshotRestoreTaskArg, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorSnapshotRestoreTaskArg, String> job(VisorSnapshotRestoreTaskArg arg) {
        switch (arg.jobAction()) {
            case START:
                return new VisorSnapshotStartRestoreJob(arg, debug);

            case CANCEL:
                return new VisorSnapshotRestoreCancelJob(arg, debug);

            case STATUS:
                return new VisorSnapshotRestoreStatusJob(arg, debug);

            default:
                throw new IllegalArgumentException("Action is not supported: " + arg.jobAction());
        }
    }

    /** */
    private static class VisorSnapshotStartRestoreJob extends VisorJob<VisorSnapshotRestoreTaskArg, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Restore task argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotStartRestoreJob(VisorSnapshotRestoreTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(VisorSnapshotRestoreTaskArg arg) throws IgniteException {
            IgniteFuture<Void> fut =
                ignite.context().cache().context().snapshotMgr().restoreSnapshot(arg.snapshotName(), arg.groupNames());

            if (fut.isDone())
                fut.get();

            return "Snapshot cache group restore operation started [snapshot=" + arg.snapshotName() +
                (arg.groupNames() == null ? "" : ", group(s)=" + F.concat(arg.groupNames(), ",")) + ']';
        }
    }

    /** */
    private static class VisorSnapshotRestoreCancelJob extends VisorJob<VisorSnapshotRestoreTaskArg, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Restore task argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotRestoreCancelJob(VisorSnapshotRestoreTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(VisorSnapshotRestoreTaskArg arg) throws IgniteException {
            boolean stopped = ignite.snapshot().cancelSnapshotRestore(arg.snapshotName()).get();

            return "Snapshot cache group restore operation " +
                (stopped ? "canceled" : "is NOT running") + " [snapshot=" + arg.snapshotName() + ']';
        }
    }

    /** */
    private static class VisorSnapshotRestoreStatusJob extends VisorJob<VisorSnapshotRestoreTaskArg, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Restore task argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotRestoreStatusJob(VisorSnapshotRestoreTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected String run(VisorSnapshotRestoreTaskArg arg) throws IgniteException {
            Map<UUID, SnapshotRestoreStatusDetails> nodesStatus =
                ignite.context().cache().context().snapshotMgr().clusterRestoreStatus(arg.snapshotName()).get();

            long clusterProcParts = 0;
            long clusterProcBytes = 0;
            long globalStartTime = 0;
            long globalEndTime = 0;
            long clusterParts = 0;
            long clusterBytes = 0;
            String errMsg = null;
            String grps = null;
            UUID reqId = null;

            if (nodesStatus == null)
                return "No information about restoring snapshot \"" + arg.snapshotName() + "\" is available.";

            SB buf = new SB();

            for (Map.Entry<UUID, SnapshotRestoreStatusDetails> e : nodesStatus.entrySet()) {
                SnapshotRestoreStatusDetails details = e.getValue();

                if (globalStartTime == 0 || globalStartTime > details.startTime()) {
                    globalStartTime = details.startTime();
                    errMsg = details.errorMessage();
                    globalEndTime = details.endTime();
                    reqId = details.requestId();
                }

                if (grps == null && !F.isEmpty(details.cacheGroupNames()))
                    grps = details.cacheGroupNames();

                long procParts = details.processedParts();
                long totalParts = details.totalParts();
                long procBytes = details.processedBytes();
                long totalBytes = details.totalBytes();

                clusterParts += totalParts;
                clusterBytes += totalBytes;
                clusterProcParts += procParts;
                clusterProcBytes += procBytes;

                if (totalBytes == 0)
                    continue;

                buf.a("  Node ").a(e.getKey()).a(": ").a(fprmatProgress(procParts, totalParts, procBytes, totalBytes));
            }

            boolean err = errMsg != null;
            String nodesInfo = buf.toString();

            buf.setLength(0);

            buf.a("Restore operation for snapshot \"").a(arg.snapshotName()).a("\" ")
                .a(err ? "failed" : (globalEndTime == 0 ? " is still in progress" : "completed successfully"))
                .a(" (requestId=").a(reqId).a(").").a(System.lineSeparator()).a(System.lineSeparator());

            if (err)
                buf.a("  Error: ").a(errMsg).a(System.lineSeparator());
            else if (clusterBytes != 0)
                buf.a("  Progress: ").a(fprmatProgress(clusterProcParts, clusterParts, clusterProcBytes, clusterBytes));

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            buf.a("  Started: ").a(dateFormat.format(new Date(globalStartTime))).a(System.lineSeparator());

            if (globalEndTime != 0)
                buf.a("  Finished: ").a(dateFormat.format(new Date(globalEndTime))).a(System.lineSeparator());

            if (grps != null)
                buf.a("  Cache groups: ").a(grps).a(System.lineSeparator());

            buf.a(System.lineSeparator()).a(nodesInfo);

            return buf.toString();
        }

        /**
         * @param procParts Processed partitions.
         * @param totalParts Total partitions.
         * @param procBytes Size in bytes of processed partitions.
         * @param totalBytes Total partitions size.
         */
        private String fprmatProgress(long procParts, long totalParts, long procBytes, long totalBytes) {
            long base = 1024L;

            int exponent = Math.max((int)(Math.log(totalBytes) / Math.log(base)), 0);
            String unit = String.valueOf(" KMGTPE".charAt(exponent)).trim();
            double baseBound = Math.pow(base, exponent);

            return new SB().a(procBytes * 100 / totalBytes).a("% completed (")
                .a(procParts).a('/').a(totalParts).a(" partitions, ")
                .a(String.format((Locale)null, "%.1f/%.1f %sB)", procBytes / baseBound, totalBytes / baseBound, unit))
                .a(System.lineSeparator()).toString();
        }
    }
}
