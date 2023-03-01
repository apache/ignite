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

package org.apache.ignite.internal.visor.cdc;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.record.CdcDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

/**
 * CDC flush caches task. Iterates over caches and writes data records to the WAL to get captured by CDC.
 */
@GridInternal
public class VisorCdcFlushCachesTask extends VisorMultiNodeTask<VisorCdcFlushCachesTaskArg, Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorCdcFlushCachesTaskArg, Void> job(VisorCdcFlushCachesTaskArg arg) {
        return new VisorCdcFlushCachesJob(arg, false);
    }

    /** {@inheritDoc} */
    @Override protected @Nullable Void reduce0(List<ComputeJobResult> results) throws IgniteException {
        for (ComputeJobResult res : results) {
            if (res.getException() != null) {
                throw new IgniteException("Failed to flush cache on the node " +
                    "[nodeId=" + res.getNode().id() + ']', res.getException());
            }
        }

        return null;
    }

    /** */
    private static class VisorCdcFlushCachesJob extends VisorJob<VisorCdcFlushCachesTaskArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Injected logger. */
        @LoggerResource
        protected IgniteLogger log;

        /** */
        private IgniteWriteAheadLogManager wal;

        /**
         * @param arg   Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorCdcFlushCachesJob(VisorCdcFlushCachesTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(VisorCdcFlushCachesTaskArg arg) throws IgniteException {
            if (F.isEmpty(arg.caches()))
                throw new IllegalArgumentException("Caches are not specified.");

            Collection<String> clusterCaches = ignite.context().cache().cacheNames();

            arg.caches().forEach(name -> {
                if (!clusterCaches.contains(name))
                    throw new IllegalArgumentException("Cache does not exist [name=" + name + ']');
            });

            wal = ignite.context().cache().context().wal(true);

            try {
                for (String name : arg.caches())
                    flushCache(name);

                wal.flush(null, true);

                log.info("Flush caches job finished successful [caches=" + arg.caches() + ']');
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }

            return null;
        }

        /**
         * @param name Cache name to flush.
         */
        private void flushCache(String name) throws IgniteCheckedException {
            GridKernalContext ctx = ignite.context();

            IgniteInternalCache<?, ?> cache = ctx.cache().cache(name);

            if (cache == null)
                throw new IgniteException("Cache does not exist [name=" + name + ']');

            GridCacheContext<?, ?> cctx = cache.context();

            GridIterator<CacheDataRow> localRows = cctx.offheap()
                .cacheIterator(cctx.cacheId(), true, false, AffinityTopologyVersion.NONE, null, null);

            for (CacheDataRow row : localRows) {
                KeyCacheObject key = row.key();

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
            }

            log.info("Cache flushed [name=" + name + ']');
        }
    }
}
