/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.management.dump;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotPartitionsVerifyTaskResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_SNAPSHOT;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.systemPermissions;

/** */
@GridInternal
public class DumpCheckTask extends VisorOneNodeTask<DumpCreateCommandArg, SnapshotPartitionsVerifyTaskResult> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<DumpCreateCommandArg, SnapshotPartitionsVerifyTaskResult> job(DumpCreateCommandArg arg) {
        return new DumpCheckJob(arg);
    }

    /** */
    private static class DumpCheckJob extends VisorJob<DumpCreateCommandArg, SnapshotPartitionsVerifyTaskResult> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** */
        protected DumpCheckJob(@Nullable DumpCreateCommandArg arg) {
            super(arg, false);
        }

        /** {@inheritDoc} */
        @Override protected SnapshotPartitionsVerifyTaskResult run(@Nullable DumpCreateCommandArg arg) throws IgniteException {
            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

            return new IgniteFutureImpl<>(snpMgr.checkSnapshot(arg.name(), null, 0)).get();
        }

        /** {@inheritDoc} */
        @Override public SecurityPermissionSet requiredPermissions() {
            return systemPermissions(ADMIN_SNAPSHOT);
        }
    }
}
