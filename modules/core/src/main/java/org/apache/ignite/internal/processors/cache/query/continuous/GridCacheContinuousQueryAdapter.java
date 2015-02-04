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

package org.apache.ignite.internal.processors.cache.query.continuous;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.continuous.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.security.*;
import org.jetbrains.annotations.*;

import javax.cache.event.*;
import java.util.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.cache.CacheMode.*;

/**
 * Continuous query implementation.
 */
public class GridCacheContinuousQueryAdapter<K, V> implements CacheContinuousQuery<K, V> {
    /** Guard. */
    private final GridBusyLock guard = new GridBusyLock();

    /** Close lock. */
    private final Lock closeLock = new ReentrantLock();

    /** Cache context. */
    private final GridCacheContext<K, V> ctx;

    /** Topic for ordered messages. */
    private final Object topic;

    /** Projection predicate */
    private final IgnitePredicate<CacheEntry<K, V>> prjPred;

    /** Keep portable flag. */
    private final boolean keepPortable;

    /** Logger. */
    private final IgniteLogger log;

    /** Local callback. */
    private volatile IgniteBiPredicate<UUID, Collection<Map.Entry<K, V>>> cb;

    /** Local callback. */
    private volatile IgniteBiPredicate<UUID, Collection<CacheContinuousQueryEntry<K, V>>> locCb;

    /** Filter. */
    private volatile IgniteBiPredicate<K, V> filter;

    /** Remote filter. */
    private volatile IgnitePredicate<CacheContinuousQueryEntry<K, V>> rmtFilter;

    /** Buffer size. */
    private volatile int bufSize = DFLT_BUF_SIZE;

    /** Time interval. */
    @SuppressWarnings("RedundantFieldInitialization")
    private volatile long timeInterval = DFLT_TIME_INTERVAL;

    /** Automatic unsubscribe flag. */
    private volatile boolean autoUnsubscribe = DFLT_AUTO_UNSUBSCRIBE;

    /** Continuous routine ID. */
    private UUID routineId;

    /**
     * @param ctx Cache context.
     * @param topic Topic for ordered messages.
     * @param prjPred Projection predicate.
     */
    GridCacheContinuousQueryAdapter(GridCacheContext<K, V> ctx, Object topic,
        @Nullable IgnitePredicate<CacheEntry<K, V>> prjPred) {
        assert ctx != null;
        assert topic != null;

        this.ctx = ctx;
        this.topic = topic;
        this.prjPred = prjPred;

        keepPortable = ctx.keepPortable();

        log = ctx.logger(getClass());
    }

    /** {@inheritDoc} */
    @Override public void callback(final IgniteBiPredicate<UUID, Collection<Map.Entry<K, V>>> cb) {
        if (cb != null) {
            this.cb = cb;

            localCallback(new CallbackWrapper<>(cb));
        }
        else
            localCallback(null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteBiPredicate<UUID, Collection<Map.Entry<K, V>>> callback() {
        return cb;
    }

    /** {@inheritDoc} */
    @Override public void filter(final IgniteBiPredicate<K, V> filter) {
        if (filter != null) {
            this.filter = filter;

            remoteFilter(new FilterWrapper<>(filter));
        }
        else
            remoteFilter(null);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteBiPredicate<K, V> filter() {
        return filter;
    }

    /** {@inheritDoc} */
    @Override public void localCallback(IgniteBiPredicate<UUID, Collection<CacheContinuousQueryEntry<K, V>>> locCb) {
        if (!guard.enterBusy())
            throw new IllegalStateException("Continuous query can't be changed after it was executed.");

        try {
            this.locCb = locCb;
        }
        finally {
            guard.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteBiPredicate<UUID, Collection<CacheContinuousQueryEntry<K, V>>> localCallback() {
        return locCb;
    }

    /** {@inheritDoc} */
    @Override public void remoteFilter(@Nullable IgnitePredicate<CacheContinuousQueryEntry<K, V>> rmtFilter) {
        if (!guard.enterBusy())
            throw new IllegalStateException("Continuous query can't be changed after it was executed.");

        try {
            this.rmtFilter = rmtFilter;
        }
        finally {
            guard.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgnitePredicate<CacheContinuousQueryEntry<K, V>> remoteFilter() {
        return rmtFilter;
    }

    /** {@inheritDoc} */
    @Override public void bufferSize(int bufSize) {
        A.ensure(bufSize > 0, "bufSize > 0");

        if (!guard.enterBusy())
            throw new IllegalStateException("Continuous query can't be changed after it was executed.");

        try {
            this.bufSize = bufSize;
        }
        finally {
            guard.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public int bufferSize() {
        return bufSize;
    }

    /** {@inheritDoc} */
    @Override public void timeInterval(long timeInterval) {
        A.ensure(timeInterval >= 0, "timeInterval >= 0");

        if (!guard.enterBusy())
            throw new IllegalStateException("Continuous query can't be changed after it was executed.");

        try {
            this.timeInterval = timeInterval;
        }
        finally {
            guard.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public long timeInterval() {
        return timeInterval;
    }

    /** {@inheritDoc} */
    @Override public void autoUnsubscribe(boolean autoUnsubscribe) {
        this.autoUnsubscribe = autoUnsubscribe;
    }

    /** {@inheritDoc} */
    @Override public boolean isAutoUnsubscribe() {
        return autoUnsubscribe;
    }

    /** {@inheritDoc} */
    @Override public void execute() throws IgniteCheckedException {
        execute(null, false, false, false, true);
    }

    /** {@inheritDoc} */
    @Override public void execute(@Nullable ClusterGroup prj) throws IgniteCheckedException {
        execute(prj, false, false, false, true);
    }

    /**
     * Starts continuous query execution.
     *
     * @param prj Grid projection.
     * @param internal If {@code true} then query notified about internal entries updates.
     * @param entryLsnr {@code True} if query created for {@link CacheEntryListener}.
     * @param sync {@code True} if query created for synchronous {@link CacheEntryListener}.
     * @param oldVal {@code True} if old value is required.
     * @throws IgniteCheckedException If failed.
     */
    public void execute(@Nullable ClusterGroup prj,
        boolean internal,
        boolean entryLsnr,
        boolean sync,
        boolean oldVal) throws IgniteCheckedException {
        if (locCb == null)
            throw new IllegalStateException("Mandatory local callback is not set for the query: " + this);

        ctx.checkSecurity(GridSecurityPermission.CACHE_READ);

        if (prj == null)
            prj = ctx.grid();

        prj = prj.forCacheNodes(ctx.name());

        if (prj.nodes().isEmpty())
            throw new ClusterTopologyCheckedException("Failed to execute query (projection is empty): " + this);

        CacheMode mode = ctx.config().getCacheMode();

        if (mode == LOCAL || mode == REPLICATED) {
            Collection<ClusterNode> nodes = prj.nodes();

            ClusterNode node = nodes.contains(ctx.localNode()) ? ctx.localNode() : F.rand(nodes);

            assert node != null;

            if (nodes.size() > 1) {
                if (node.id().equals(ctx.localNodeId()))
                    U.warn(log, "Continuous query for " + mode + " cache can be run only on local node. " +
                        "Will execute query locally: " + this);
                else
                    U.warn(log, "Continuous query for " + mode + " cache can be run only on single node. " +
                        "Will execute query on remote node [qry=" + this + ", node=" + node + ']');
            }

            prj = prj.forNode(node);
        }

        closeLock.lock();

        try {
            if (routineId != null)
                throw new IllegalStateException("Continuous query can't be executed twice.");

            guard.block();

            int taskNameHash =
                ctx.kernalContext().security().enabled() ? ctx.kernalContext().job().currentTaskNameHash() : 0;

            GridContinuousHandler hnd = new GridCacheContinuousQueryHandler<>(ctx.name(),
                topic,
                locCb,
                rmtFilter,
                prjPred,
                internal,
                entryLsnr,
                sync,
                oldVal,
                taskNameHash,
                keepPortable);

            routineId = ctx.kernalContext().continuous().startRoutine(hnd,
                bufSize,
                timeInterval,
                autoUnsubscribe,
                prj.predicate()).get();
        }
        finally {
            closeLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteCheckedException {
        closeLock.lock();

        try {
            if (routineId != null)
                ctx.kernalContext().continuous().stopRoutine(routineId).get();
        }
        finally {
            closeLock.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheContinuousQueryAdapter.class, this);
    }

    /**
     * Deprecated callback wrapper.
     */
    static class CallbackWrapper<K, V> implements IgniteBiPredicate<UUID, Collection<CacheContinuousQueryEntry<K, V>>> {
        /** Serialization ID. */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteBiPredicate<UUID, Collection<Map.Entry<K, V>>> cb;

        /**
         * @param cb Deprecated callback.
         */
        private CallbackWrapper(IgniteBiPredicate<UUID, Collection<Map.Entry<K, V>>> cb) {
            this.cb = cb;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public boolean apply(UUID nodeId, Collection<CacheContinuousQueryEntry<K, V>> entries) {
            return cb.apply(nodeId, (Collection<Map.Entry<K,V>>)(Collection)entries);
        }
    }

    /**
     * Deprecated filter wrapper.
     */
    static class FilterWrapper<K, V> implements IgnitePredicate<CacheContinuousQueryEntry<K, V>> {
        /** Serialization ID. */
        private static final long serialVersionUID = 0L;

        /** */
        private final IgniteBiPredicate<K, V> filter;

        /**
         * @param filter Deprecated callback.
         */
        FilterWrapper(IgniteBiPredicate<K, V> filter) {
            this.filter = filter;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public boolean apply(CacheContinuousQueryEntry<K, V> entry) {
            return filter.apply(entry.getKey(), entry.getValue());
        }
    }
}
