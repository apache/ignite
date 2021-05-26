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

import java.util.Collection;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSnapshot;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteFuture;

/**
 * @see IgniteSnapshot#restoreSnapshot(String, Collection)
 */
@GridInternal
public class VisorSnapshotRestoreTask extends VisorOneNodeTask<VisorSnapshotRestoreTaskArg, String> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorSnapshotRestoreTaskArg, String> job(VisorSnapshotRestoreTaskArg arg) {
        return new VisorSnapshotRestoreJob(arg, debug);
    }

    /** */
    private static class VisorSnapshotRestoreJob extends VisorJob<VisorSnapshotRestoreTaskArg, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Restore task argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotRestoreJob(VisorSnapshotRestoreTaskArg arg, boolean debug) {
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
}
