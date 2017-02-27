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

package org.apache.ignite.internal.util.nio;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Implements basic lifecycle for communication clients.
 */
public abstract class GridAbstractCommunicationClient implements GridCommunicationClient {
    /** Time when this client was last used. */
    private volatile long lastUsed = U.currentTimeMillis();

    /** Reservations. */
    private final AtomicBoolean closed = new AtomicBoolean();

    /** Metrics listener. */
    protected final GridNioMetricsListener metricsLsnr;

    /** */
    private final int connIdx;

    /**
     * @param connIdx Connection index.
     * @param metricsLsnr Metrics listener.
     */
    protected GridAbstractCommunicationClient(int connIdx, @Nullable GridNioMetricsListener metricsLsnr) {
        this.connIdx = connIdx;
        this.metricsLsnr = metricsLsnr;
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
        closed.set(false);
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
