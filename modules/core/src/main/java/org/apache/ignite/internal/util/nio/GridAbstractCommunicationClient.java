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

import java.util.concurrent.atomic.AtomicInteger;
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
    private final AtomicInteger reserves = new AtomicInteger();

    /** Metrics listener. */
    protected final GridNioMetricsListener metricsLsnr;

    /**
     * @param metricsLsnr Metrics listener.
     */
    protected GridAbstractCommunicationClient(@Nullable GridNioMetricsListener metricsLsnr) {
        this.metricsLsnr = metricsLsnr;
    }

    /** {@inheritDoc} */
    @Override public boolean close() {
        return reserves.compareAndSet(0, -1);
    }

    /** {@inheritDoc} */
    @Override public void forceClose() {
        reserves.set(-1);
    }

    /** {@inheritDoc} */
    @Override public boolean closed() {
        return reserves.get() == -1;
    }

    /** {@inheritDoc} */
    @Override public boolean reserve() {
        while (true) {
            int r = reserves.get();

            if (r == -1)
                return false;

            if (reserves.compareAndSet(r, r + 1))
                return true;
        }
    }

    /** {@inheritDoc} */
    @Override public void release() {
        while (true) {
            int r = reserves.get();

            if (r == -1)
                return;

            if (reserves.compareAndSet(r, r - 1))
                return;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean reserved() {
        return reserves.get() > 0;
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