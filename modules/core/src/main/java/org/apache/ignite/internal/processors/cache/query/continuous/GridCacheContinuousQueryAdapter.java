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

import static org.apache.ignite.cache.CacheDistributionMode.*;

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
            throw new ClusterTopologyCheckedException("Failed to continuous execute query (projection is empty): " +
                this);

        boolean skipPrimaryCheck = false;

        Collection<ClusterNode> nodes = prj.nodes();

        if (nodes.isEmpty())
            throw new ClusterTopologyCheckedException("Failed to execute continuous query (empty projection is " +
                "provided): " + this);

        switch (ctx.config().getCacheMode()) {
            case LOCAL:
                if (!nodes.contains(ctx.localNode()))
                    throw new ClusterTopologyCheckedException("Continuous query for LOCAL cache can be executed " +
                        "only locally (provided projection contains remote nodes only): " + this);
                else if (nodes.size() > 1)
                    U.warn(log, "Continuous query for LOCAL cache will be executed locally (provided projection is " +
                        "ignored): " + this);

                prj = prj.forNode(ctx.localNode());

                break;

            case REPLICATED:
                if (nodes.size() == 1 && F.first(nodes).equals(ctx.localNode())) {
                    CacheDistributionMode distributionMode = ctx.config().getDistributionMode();

                    if (distributionMode == PARTITIONED_ONLY || distributionMode == NEAR_PARTITIONED)
                        skipPrimaryCheck = true;
                }

                break;
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
                skipPrimaryCheck,
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
}
