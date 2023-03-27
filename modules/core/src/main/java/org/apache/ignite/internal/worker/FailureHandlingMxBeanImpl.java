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

package org.apache.ignite.internal.worker;

import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.mxbean.FailureHandlingMxBean;
import org.jetbrains.annotations.NotNull;

/** {@inheritDoc} */
public class FailureHandlingMxBeanImpl implements FailureHandlingMxBean {
    /** System worker registry. */
    private final WorkersRegistry workerRegistry;

    /** Database manager. */
    private final IgniteCacheDatabaseSharedManager dbMgr;

    /**
     * @param workersRegistry Workers registry.
     * @param dbMgr Database manager.
     */
    public FailureHandlingMxBeanImpl(
        @NotNull WorkersRegistry workersRegistry,
        @NotNull IgniteCacheDatabaseSharedManager dbMgr
    ) {
        this.workerRegistry = workersRegistry;
        this.dbMgr = dbMgr;
    }

    /** {@inheritDoc} */
    @Override public boolean getLivenessCheckEnabled() {
        return workerRegistry.livenessCheckEnabled();
    }

    /** {@inheritDoc} */
    @Override public void setLivenessCheckEnabled(boolean val) {
        workerRegistry.livenessCheckEnabled(val);
    }

    /** {@inheritDoc} */
    @Override public long getSystemWorkerBlockedTimeout() {
        return workerRegistry.getSystemWorkerBlockedTimeout();
    }

    /** {@inheritDoc} */
    @Override public void setSystemWorkerBlockedTimeout(long val) {
        workerRegistry.setSystemWorkerBlockedTimeout(val);
    }

    /** {@inheritDoc} */
    @Override public long getCheckpointReadLockTimeout() {
        return dbMgr.checkpointReadLockTimeout();
    }

    /** {@inheritDoc} */
    @Override public void setCheckpointReadLockTimeout(long val) {
        dbMgr.checkpointReadLockTimeout(val);
    }
}
