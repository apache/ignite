/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */
package org.apache.ignite;

/**
 * Converter class from {@link DataStorageMetrics} to legacy {@link PersistenceMetrics}.
 */
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
