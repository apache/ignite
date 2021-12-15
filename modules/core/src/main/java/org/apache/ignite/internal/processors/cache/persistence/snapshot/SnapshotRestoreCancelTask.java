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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Snapshot restore cancel task.
 */
@GridInternal
class SnapshotRestoreCancelTask extends SnapshotRestoreManagementTask<Boolean> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected ComputeJob makeJob(String snpName) {
        return new ComputeJobAdapter() {
            /** Auto-injected grid instance. */
            @IgniteInstanceResource
            private transient IgniteEx ignite;

            @Override public Object execute() throws IgniteException {
                return ignite.context().cache().context().snapshotMgr().cancelLocalRestoreTask(snpName).get();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public Boolean reduce(List<ComputeJobResult> results) throws IgniteException {
        boolean ret = false;

        for (ComputeJobResult r : results) {
            if (r.getException() != null)
                throw new IgniteException("Failed to execute job [nodeId=" + r.getNode().id() + ']', r.getException());

            ret |= Boolean.TRUE.equals(r.getData());
        }

        return ret;
    }
}
