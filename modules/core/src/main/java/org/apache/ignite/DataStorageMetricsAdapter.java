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
package org.apache.ignite;

import org.apache.ignite.internal.processors.metric.GridMetricManager;

/**
 * Converter class from {@link DataStorageMetrics} to legacy {@link PersistenceMetrics}.
 *
 * @deprecated Use {@link GridMetricManager} instead.
 */
@Deprecated
public class DataStorageMetricsAdapter implements PersistenceMetrics {
    /** Delegate. */
    private final DataStorageMetrics delegate;

    /**
     * @param delegate Delegate.
     */
    private DataStorageMetricsAdapter(DataStorageMetrics delegate) {
        this.delegate = delegate;
    }

    /**
     * @param delegate DataStorageMetrics.
     * @return Wrapped {@link DataStorageMetrics} that implements {@link PersistenceMetrics}.
     * Null value is not wrapped and returned as is.
     */
    public static DataStorageMetricsAdapter valueOf(DataStorageMetrics delegate) {
        return delegate == null ? null : new DataStorageMetricsAdapter(delegate);
    }

    /** {@inheritDoc} */
    @Override public float getWalLoggingRate() {
        return delegate.getWalLoggingRate();
    }

    /** {@inheritDoc} */
    @Override public float getWalWritingRate() {
        return delegate.getWalWritingRate();
    }

    /** {@inheritDoc} */
    @Override public int getWalArchiveSegments() {
        return delegate.getWalArchiveSegments();
    }

    /** {@inheritDoc} */
    @Override public float getWalFsyncTimeAverage() {
        return delegate.getWalFsyncTimeAverage();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointingDuration() {
        return delegate.getLastCheckpointDuration();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointLockWaitDuration() {
        return delegate.getLastCheckpointLockWaitDuration();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointMarkDuration() {
        return delegate.getLastCheckpointMarkDuration();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointPagesWriteDuration() {
        return delegate.getLastCheckpointPagesWriteDuration();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointFsyncDuration() {
        return delegate.getLastCheckpointFsyncDuration();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointTotalPagesNumber() {
        return delegate.getLastCheckpointTotalPagesNumber();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointDataPagesNumber() {
        return delegate.getLastCheckpointDataPagesNumber();
    }

    /** {@inheritDoc} */
    @Override public long getLastCheckpointCopiedOnWritePagesNumber() {
        return delegate.getLastCheckpointCopiedOnWritePagesNumber();
    }
}
