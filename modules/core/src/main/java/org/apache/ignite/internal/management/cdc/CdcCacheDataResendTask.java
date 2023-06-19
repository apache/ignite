/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.management.cdc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.CdcDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

/**
 * Task to forcefully resend all cache data to CDC.
 * Iterates over caches and writes primary copies of data entries to the WAL to get captured by CDC.
 */
@GridInternal
public class CdcCacheDataResendTask extends VisorMultiNodeTask<CdcResendCommandArg, Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Topology version when task was started. */
    private AffinityTopologyVersion topVer;

    /** {@inheritDoc} */
    @Override protected VisorJob<CdcResendCommandArg, Void> job(CdcResendCommandArg arg) {
        return new VisorCdcCacheDataResendJob(arg, topVer);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<CdcResendCommandArg> arg) {
        // Check there is no rebalance.
        GridDhtPartitionsExchangeFuture fut = ignite.context().cache().context().exchange().lastFinishedFuture();

        if (!fut.rebalanced()) {
            throw new IgniteException("CDC cache data resend cancelled. Rebalance sheduled " +
                "[topVer=" + fut.topologyVersion() + ']');
        }

        // Cancel resend if affinity will change.
        topVer = ignite.context().cache().context().exchange().lastAffinityChangedTopologyVersion(fut.topologyVersion());

        return F.nodeIds(ignite.cluster().forServers().nodes());
    }

    /** {@inheritDoc} */
    @Override protected @Nullable Void reduce0(List<ComputeJobResult> results) throws IgniteException {
        for (ComputeJobResult res : results) {
            if (res.getException() != null) {
                throw new IgniteException("CDC cache data resend cancelled. Failed to resend cache data " +
                    "on the node [nodeId=" + res.getNode().id() + ']', res.getException());
            }
        }

        return null;
    }

    /** */
    private static class VisorCdcCacheDataResendJob extends VisorJob<CdcResendCommandArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Injected logger. */
        @LoggerResource
        protected IgniteLogger log;

        /** */
        private IgniteWriteAheadLogManager wal;

        /** */
        private GridCachePartitionExchangeManager<Object, Object> exchange;

        /** Topology version when task was started. */
        private final AffinityTopologyVersion topVer;

        /** */
        private GridDhtPartitionsExchangeFuture lastFut;

        /**
         * @param arg Job argument.
         * @param topVer Topology version when task was started.
         */
        protected VisorCdcCacheDataResendJob(CdcResendCommandArg arg, AffinityTopologyVersion topVer) {
            super(arg, false);

            this.topVer = topVer;
        }

        /** {@inheritDoc} */
        @Override protected Void run(CdcResendCommandArg arg) throws IgniteException {
            if (F.isEmpty(arg.caches()))
                throw new IllegalArgumentException("Caches are not specified.");

            List<IgniteInternalCache<?, ?>> caches = new ArrayList<>();

            for (String name : arg.caches()) {
                IgniteInternalCache<?, ?> cache = ignite.context().cache().cache(name);

                if (cache == null)
                    throw new IgniteException("Cache does not exist [cacheName=" + name + ']');

                if (!cache.context().dataRegion().config().isCdcEnabled()) {
                    throw new IgniteException("CDC is not enabled for given cache [cacheName=" + name +
                        ", dataRegionName=" + cache.context().dataRegion().config().getName() + ']');
                }

                if (cache.context().mvccEnabled())
                    throw new UnsupportedOperationException("The TRANSACTIONAL_SNAPSHOT mode is not supported.");

                caches.add(cache);
            }

            if (log.isInfoEnabled())
                log.info("CDC cache data resend started [caches=" + String.join(", ", arg.caches()) + ']');

            wal = ignite.context().cache().context().wal(true);
            exchange = ignite.context().cache().context().exchange();

            try {
                Iterator<IgniteInternalCache<?, ?>> iter = caches.iterator();

                while (iter.hasNext() && !isCancelled())
                    resendCacheData(iter.next());

                wal.flush(null, true);

                if (log.isInfoEnabled()) {
                    log.info("CDC cache data resend " + (isCancelled() ? "cancelled" : "finished") +
                        " [caches=" + String.join(", ", arg.caches()) + ']');
                }
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            return null;
        }

        /** @param cache Cache. */
        private void resendCacheData(IgniteInternalCache<?, ?> cache) throws IgniteCheckedException {
            if (log.isInfoEnabled())
                log.info("CDC cache data resend started [cacheName=" + cache.name() + ']');

            GridCacheContext<?, ?> cctx = cache.context();

            GridIterator<CacheDataRow> localRows = cctx.offheap()
                .cacheIterator(cctx.cacheId(), true, false, AffinityTopologyVersion.NONE, null, null);

            long cnt = 0;
            Set<Integer> parts = new TreeSet<>();

            for (CacheDataRow row : localRows) {
                if (isCancelled())
                    break;

                ensureTopologyNotChanged();

                KeyCacheObject key = row.key();

                if (log.isTraceEnabled())
                    log.trace("Resend key: " + key);

                CdcDataRecord rec = new CdcDataRecord(new DataEntry(
                    cctx.cacheId(),
                    key,
                    row.value(),
                    GridCacheOperation.CREATE,
                    null,
                    row.version(),
                    row.expireTime(),
                    key.partition(),
                    -1,
                    DataEntry.flags(true))
                );

                wal.log(rec);

                parts.add(key.partition());

                if ((++cnt % 1_000 == 0) && log.isDebugEnabled())
                    log.debug("Resend entries count: " + cnt);
            }

            if (log.isInfoEnabled()) {
                if (isCancelled())
                    log.info("CDC cache data resend cancelled.");
                else {
                    log.info("CDC cache data resend finished [cacheName=" + cache.name() +
                        ", entriesCnt=" + cnt + ", parts=" + parts + ']');
                }
            }
        }

        /** */
        private void ensureTopologyNotChanged() {
            GridDhtPartitionsExchangeFuture fut = exchange.lastFinishedFuture();

            if (lastFut != fut) {
                AffinityTopologyVersion lastChanged = exchange.lastAffinityChangedTopologyVersion(fut.topologyVersion());

                if (!topVer.equals(lastChanged)) {
                    throw new IgniteException("CDC cache data resend cancelled. Topology changed during resend " +
                        "[startTopVer=" + topVer + ", currentTopVer=" + fut.topologyVersion() + ']');
                }

                lastFut = fut;
            }
        }
    }
}
