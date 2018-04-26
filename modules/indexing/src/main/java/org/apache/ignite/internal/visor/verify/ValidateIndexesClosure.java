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
package org.apache.ignite.internal.visor.verify;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectUtils;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.verify.PartitionKey;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;

/**
 *
 */
public class ValidateIndexesClosure implements IgniteCallable<Map<PartitionKey, ValidateIndexesPartitionResult>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Ignite. */
    @IgniteInstanceResource
    private transient IgniteEx ignite;

    /** Injected logger. */
    @LoggerResource
    private IgniteLogger log;

    /** Cache names. */
    private Set<String> cacheNames;

    /** Counter of processed partitions. */
    private final AtomicInteger completionCntr = new AtomicInteger(0);

    /** Calculation executor. */
    private volatile ExecutorService calcExecutor;

    /**
     * @param cacheNames Cache names.
     */
    public ValidateIndexesClosure(Set<String> cacheNames) {
        this.cacheNames = cacheNames;
    }

    /** {@inheritDoc} */
    @Override public Map<PartitionKey, ValidateIndexesPartitionResult> call() throws Exception {
        calcExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        try {
            return call0();
        }
        finally {
            calcExecutor.shutdown();
        }
    }

    /**
     *
     */
    private Map<PartitionKey, ValidateIndexesPartitionResult> call0() throws Exception {
        Set<Integer> grpIds = new HashSet<>();

        Set<String> missingCaches = new HashSet<>();

        if (cacheNames != null) {
            for (String cacheName : cacheNames) {
                DynamicCacheDescriptor desc = ignite.context().cache().cacheDescriptor(cacheName);

                if (desc == null) {
                    missingCaches.add(cacheName);

                    continue;
                }

                grpIds.add(desc.groupId());
            }

            if (!missingCaches.isEmpty()) {
                StringBuilder strBuilder = new StringBuilder("The following caches do not exist: ");

                for (String name : missingCaches)
                    strBuilder.append(name).append(", ");

                strBuilder.delete(strBuilder.length() - 2, strBuilder.length());

                throw new IgniteException(strBuilder.toString());
            }
        }
        else {
            Collection<CacheGroupContext> groups = ignite.context().cache().cacheGroups();

            for (CacheGroupContext grp : groups) {
                if (!grp.systemCache() && !grp.isLocal())
                    grpIds.add(grp.groupId());
            }
        }

        List<Future<Map<PartitionKey, ValidateIndexesPartitionResult>>> procPartFutures = new ArrayList<>();

        completionCntr.set(0);

        for (Integer grpId : grpIds) {
            CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(grpId);

            if (grpCtx == null)
                continue;

            List<GridDhtLocalPartition> parts = grpCtx.topology().localPartitions();

            for (GridDhtLocalPartition part : parts)
                procPartFutures.add(processPartitionAsync(grpCtx, part));
        }

        Map<PartitionKey, ValidateIndexesPartitionResult> res = new HashMap<>();

        long lastProgressLogTs = U.currentTimeMillis();

        for (int i = 0; i < procPartFutures.size(); ) {
            Future<Map<PartitionKey, ValidateIndexesPartitionResult>> fut = procPartFutures.get(i);

            try {
                Map<PartitionKey, ValidateIndexesPartitionResult> partRes = fut.get(1, TimeUnit.SECONDS);

                res.putAll(partRes);

                i++;
            }
            catch (InterruptedException | ExecutionException e) {
                for (int j = i + 1; j < procPartFutures.size(); j++)
                    procPartFutures.get(j).cancel(false);

                if (e instanceof InterruptedException)
                    throw new IgniteInterruptedException((InterruptedException)e);
                else if (e.getCause() instanceof IgniteException)
                    throw (IgniteException)e.getCause();
                else
                    throw new IgniteException(e.getCause());
            }
            catch (TimeoutException ignored) {
                if (U.currentTimeMillis() - lastProgressLogTs > 60 * 1000L) {
                    lastProgressLogTs = U.currentTimeMillis();

                    log.warning("ValidateIndexesClosure is still running, processed " + completionCntr.get() + " of " +
                        procPartFutures.size() + " local partitions");
                }
            }
        }

        return res;
    }

    /**
     * @param grpCtx Group context.
     * @param part Local partition.
     */
    private Future<Map<PartitionKey, ValidateIndexesPartitionResult>> processPartitionAsync(
        final CacheGroupContext grpCtx,
        final GridDhtLocalPartition part
    ) {
        return calcExecutor.submit(new Callable<Map<PartitionKey, ValidateIndexesPartitionResult>>() {
            @Override public Map<PartitionKey, ValidateIndexesPartitionResult> call() throws Exception {
                return processPartition(grpCtx, part);
            }
        });
    }

    /**
     * @param grpCtx Group context.
     * @param part Local partition.
     */
    private Map<PartitionKey, ValidateIndexesPartitionResult> processPartition(
        CacheGroupContext grpCtx,
        GridDhtLocalPartition part
    ) {
        if (!part.reserve())
            return Collections.emptyMap();

        ValidateIndexesPartitionResult partRes;

        try {
            if (part.state() != GridDhtPartitionState.OWNING)
                return Collections.emptyMap();

            long updateCntrBefore = part.updateCounter();

            long partSize = part.dataStore().fullSize();

            GridIterator<CacheDataRow> it = grpCtx.offheap().partitionIterator(part.id());

            Object consId = ignite.context().discovery().localNode().consistentId();

            boolean isPrimary = part.primary(grpCtx.topology().readyTopologyVersion());

            partRes = new ValidateIndexesPartitionResult(updateCntrBefore, partSize, isPrimary, consId);

            boolean enoughIssues = false;

            long keysProcessed = 0;
            long lastProgressLog = U.currentTimeMillis();

            while (it.hasNextX()) {
                if (enoughIssues)
                    break;

                CacheDataRow row = it.nextX();

                int cacheId = row.cacheId() == 0 ? grpCtx.groupId() : row.cacheId();

                GridCacheContext cacheCtx = row.cacheId() == 0 ?
                    grpCtx.singleCacheContext() : grpCtx.shared().cacheContext(row.cacheId());

                if (cacheCtx == null)
                    throw new IgniteException("Unknown cacheId of CacheDataRow: " + cacheId);

                GridQueryProcessor qryProcessor = ignite.context().query();

                try {
                    Method m = GridQueryProcessor.class.getDeclaredMethod("typeByValue", String.class,
                        CacheObjectContext.class, KeyCacheObject.class, CacheObject.class, boolean.class);

                    m.setAccessible(true);

                    QueryTypeDescriptorImpl res = (QueryTypeDescriptorImpl)m.invoke(
                        qryProcessor, cacheCtx.name(), cacheCtx.cacheObjectContext(), row.key(), row.value(), true);

                    if (res == null)
                        continue; // Tolerate - (k, v) is just not indexed.

                    IgniteH2Indexing indexing = (IgniteH2Indexing)qryProcessor.getIndexing();

                    GridH2Table gridH2Tbl = indexing.dataTable(cacheCtx.name(), res.tableName());

                    if (gridH2Tbl == null)
                        continue; // Tolerate - (k, v) is just not indexed.

                    GridH2RowDescriptor gridH2RowDesc = gridH2Tbl.rowDescriptor();

                    GridH2Row h2Row = gridH2RowDesc.createRow(row);

                    ArrayList<Index> indexes = gridH2Tbl.getIndexes();

                    for (Index idx : indexes) {
                        try {
                            Cursor cursor = idx.find((Session) null, h2Row, h2Row);

                            if (cursor == null || !cursor.next())
                                throw new IgniteCheckedException("Key not found.");
                        }
                        catch (Throwable t) {
                            Object o = CacheObjectUtils.unwrapBinaryIfNeeded(
                                grpCtx.cacheObjectContext(), row.key(), true, true);

                            IndexValidationIssue is = new IndexValidationIssue(
                                o.toString(), cacheCtx.name(), idx.getName(), t);

                            log.error("Failed to lookup key: " + is.toString());

                            enoughIssues |= partRes.reportIssue(is);
                        }
                    }
                }
                catch (IllegalAccessException | NoSuchMethodException e) {
                    log.error("Failed to invoke typeByValue", e);

                    throw new IgniteException(e);
                }
                catch (InvocationTargetException e) {
                    Throwable target = e.getTargetException();

                    log.error("Failed to invoke typeByValue", target);

                    throw new IgniteException(target);
                }
                finally {
                    keysProcessed++;

                    if (U.currentTimeMillis() - lastProgressLog >= 60_000 && partSize > 0) {
                        log.warning("Processing partition " + part.id() + " (" + (keysProcessed * 100 / partSize) +
                            "% " + keysProcessed + "/" + partSize + ")");

                        lastProgressLog = U.currentTimeMillis();
                    }
                }
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to process partition [grpId=" + grpCtx.groupId() +
                ", partId=" + part.id() + "]", e);

            return Collections.emptyMap();
        }
        finally {
            part.release();
        }

        PartitionKey partKey = new PartitionKey(grpCtx.groupId(), part.id(), grpCtx.cacheOrGroupName());

        completionCntr.incrementAndGet();

        return Collections.singletonMap(partKey, partRes);
    }
}
