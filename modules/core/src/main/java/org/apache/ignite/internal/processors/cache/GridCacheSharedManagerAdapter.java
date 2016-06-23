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

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Convenience adapter for cache managers.
 */
public class GridCacheSharedManagerAdapter<K, V> implements GridCacheSharedManager<K, V> {
    /** Context. */
    protected GridCacheSharedContext<K, V> cctx;

    /** Logger. */
    protected IgniteLogger log;

    /** Starting flag. */
    private final AtomicBoolean starting = new AtomicBoolean(false);

    /** */
    private final AtomicBoolean stop = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override public final void start(GridCacheSharedContext<K, V> cctx) throws IgniteCheckedException {
        if (!starting.compareAndSet(false, true))
            assert false : "Method start is called more than once for manager: " + this;

        assert cctx != null;

        this.cctx = cctx;

        log = cctx.logger(getClass());

        start0();

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /**
     * @return Logger.
     */
    protected IgniteLogger log() {
        return log;
    }

    /**
     * @return Context.
     */
    protected GridCacheSharedContext<K, V> context() {
        return cctx;
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    protected void start0() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public final void stop(boolean cancel) {
        if (!starting.get() || !stop.compareAndSet(false, true))
            // Ignoring attempt to stop manager that has never been started.
            return;

        stop0(cancel);

        if (log != null && log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * @param cancel Cancel flag.
     */
    protected void stop0(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public final void onKernalStart(boolean reconnect) throws IgniteCheckedException {
        onKernalStart0(reconnect);

        if (!reconnect && log != null && log.isDebugEnabled())
            log.debug(kernalStartInfo());
    }

    /** {@inheritDoc} */
    @Override public final void onKernalStop(boolean cancel) {
        if (!starting.get())
            // Ignoring attempt to stop manager that has never been started.
            return;

        onKernalStop0(cancel);

        if (log != null && log.isDebugEnabled())
            log.debug(kernalStopInfo());
    }

    /**
     * @param reconnect {@code True} if manager restarted after client reconnect.
     * @throws IgniteCheckedException If failed.
     */
    protected void onKernalStart0(boolean reconnect) throws IgniteCheckedException {
        // No-op.
    }

    /**
     * @param cancel Cancel flag.
     */
    protected void onKernalStop0(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        // No-op.
    }

    /**
     * @return Start info.
     */
    protected String startInfo() {
        return "Cache manager started.";
    }

    /**
     * @return Stop info.
     */
    protected String stopInfo() {
        return "Cache manager stopped.";
    }

    /**
     * @return Start info.
     */
    protected String kernalStartInfo() {
        return "Cache manager received onKernalStart() callback.";
    }

    /**
     * @return Stop info.
     */
    protected String kernalStopInfo() {
        return "Cache manager received onKernalStop() callback.";
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheSharedManagerAdapter.class, this);
    }
}