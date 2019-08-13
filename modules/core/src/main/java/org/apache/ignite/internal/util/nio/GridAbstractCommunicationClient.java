/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.nio;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.nio.GridNioServer.RECEIVED_BYTES_METRIC_DESC;
import static org.apache.ignite.internal.util.nio.GridNioServer.RECEIVED_BYTES_METRIC_NAME;
import static org.apache.ignite.internal.util.nio.GridNioServer.SENT_BYTES_METRIC_DESC;
import static org.apache.ignite.internal.util.nio.GridNioServer.SENT_BYTES_METRIC_NAME;

/**
 * Implements basic lifecycle for communication clients.
 */
public abstract class GridAbstractCommunicationClient implements GridCommunicationClient {
    /** Time when this client was last used. */
    private volatile long lastUsed = U.currentTimeMillis();

    /** Reservations. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /** */
    private final int connIdx;

    /** Received bytes count metric. */
    @Nullable protected final LongAdderMetric rcvdBytesCntMetric;

    /** Sent bytes count metric. */
    @Nullable protected final LongAdderMetric sentBytesCntMetric;

    /**
     * @param connIdx Connection index.
     * @param mreg Metrics registry.
     */
    protected GridAbstractCommunicationClient(int connIdx, @Nullable MetricRegistry mreg) {
        this.connIdx = connIdx;

        rcvdBytesCntMetric = mreg == null ?
            null : mreg.longAdderMetric(RECEIVED_BYTES_METRIC_NAME, RECEIVED_BYTES_METRIC_DESC);

        sentBytesCntMetric = mreg == null ?
            null : mreg.longAdderMetric(SENT_BYTES_METRIC_NAME, SENT_BYTES_METRIC_DESC);
    }

    /** {@inheritDoc} */
    @Override public int connectionIndex() {
        return connIdx;
    }

    /** {@inheritDoc} */
    @Override public boolean close() {
        return !closed.get() && closed.compareAndSet(false, true);
    }

    /** {@inheritDoc} */
    @Override public void forceClose() {
        closed.set(true);
    }

    /** {@inheritDoc} */
    @Override public boolean closed() {
        return closed.get();
    }

    /** {@inheritDoc} */
    @Override public boolean reserve() {
        return !closed.get();
    }

    /** {@inheritDoc} */
    @Override public void release() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public long getIdleTime() {
        return U.currentTimeMillis() - lastUsed;
    }

    /**
     * Updates used time.
     */
    protected void markUsed() {
        lastUsed = U.currentTimeMillis();
    }

    /** {@inheritDoc} */
    @Override public boolean async() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridAbstractCommunicationClient.class, this);
    }
}
