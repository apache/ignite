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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.QueryTypeDescriptorImpl;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.h2.engine.Session;
import org.h2.index.Cursor;
import org.h2.index.Index;

/**
 * Closure that locally validates indexes of given caches.
 * Validation consists of three checks:
 * 1. If entry is present in cache data tree, it's reachable from all cache SQL indexes
 * 2. If entry is present in cache SQL index, it can be dereferenced with link from index
 * 3. If entry is present in cache SQL index, it's present in cache data tree
 */
public class ValidateIndexesClosure implements IgniteCallable<VisorValidateIndexesJobResult> {
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

    /** If provided only first K elements will be validated. */
    private final int checkFirst;

    /** If provided only each Kth element will be validated. */
    private final int checkThrough;

    /** Counter of processed partitions. */
    private final AtomicInteger processedPartitions = new AtomicInteger(0);

    /** Total partitions. */
    private volatile int totalPartitions;

    /** Counter of processed indexes. */
    private final AtomicInteger processedIndexes = new AtomicInteger(0);

    /** Total partitions. */
    private volatile int totalIndexes;

    /** Last progress print timestamp. */
    private final AtomicLong lastProgressPrintTs = new AtomicLong(0);

    /** Calculation executor. */
    private volatile ExecutorService calcExecutor;

    /**
     * @param cacheNames Cache names.
     * @param checkFirst If positive only first K elements will be validated.
     * @param checkThrough If positive only each Kth element will be validated.
     */
    public ValidateIndexesClosure(Set<String> cacheNames, int checkFirst, int checkThrough) {
        this.cacheNames = cacheNames;
        this.checkFirst = checkFirst;
        this.checkThrough = checkThrough;
    }

    /** {@inheritDoc} */
    @Override public VisorValidateIndexesJobResult call() throws Exception {
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
    private VisorValidateIndexesJobResult call0() throws Exception {
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
        List<Future<Map<String, ValidateIndexesPartitionResult>>> procIdxFutures = new ArrayList<>();
        List<T2<CacheGroupContext, GridDhtLocalPartition>> partArgs = new ArrayList<>();
        List<T2<GridCacheContext, Index>> idxArgs = new ArrayList<>();

        for (Integer grpId : grpIds) {
            CacheGroupContext grpCtx = ignite.context().cache().cacheGroup(grpId);

            if (grpCtx == null)
                continue;

            List<GridDhtLocalPartition> parts = grpCtx.topology().localPartitions();

            for (GridDhtLocalPartition part : parts)
                partArgs.add(new T2<>(grpCtx, part));

            GridQueryProcessor qry = ignite.context().query();

            IgniteH2Indexing indexing = (IgniteH2Indexing)qry.getIndexing();

            for (GridCacheContext ctx : grpCtx.caches()) {
                Collection<GridQueryTypeDescriptor> types = qry.types(ctx.name());

                if (!F.isEmpty(types)) {
                    for (GridQueryTypeDescriptor type : types) {
                        GridH2Table gridH2Tbl = indexing.dataTable(ctx.name(), type.tableName());

                        if (gridH2Tbl == null)
                            continue;

                        ArrayList<Index> indexes = gridH2Tbl.getIndexes();

                        for (Index idx : indexes)
                            idxArgs.add(new T2<>(ctx, idx));
                    }
                }
            }
        }

        // To decrease contention on same indexes.
        Collections.shuffle(partArgs);
        Collections.shuffle(idxArgs);

        for (T2<CacheGroupContext, GridDhtLocalPartition> t2 : partArgs)
            procPartFutures.add(processPartitionAsync(t2.get1(), t2.get2()));

        for (T2<GridCacheContext, Index> t2 : idxArgs)
            procIdxFutures.add(processIndexAsync(t2.get1(), t2.get2()));

        totalPartitions = procPartFutures.size();
        totalIndexes = procIdxFutures.size();

        Map<PartitionKey, ValidateIndexesPartitionResult> partResults = new HashMap<>();
        Map<String, ValidateIndexesPartitionResult> idxResults = new HashMap<>();

        int curPart = 0;
        int curIdx = 0;
        try {
            for (; curPart < procPartFutures.size(); curPart++) {
                Future<Map<PartitionKey, ValidateIndexesPartitionResult>> fut = procPartFutures.get(curPart);

                Map<PartitionKey, ValidateIndexesPartitionResult> partRes = fut.get();

                partResults.putAll(partRes);
            }

            for (; curIdx < procIdxFutures.size(); curIdx++) {
                Future<Map<String, ValidateIndexesPartitionResult>> fut = procIdxFutures.get(curIdx);

                Map<String, ValidateIndexesPartitionResult> idxRes = fut.get();

                idxResults.putAll(idxRes);
            }
        }
        catch (InterruptedException | ExecutionException e) {
            for (int j = curPart; j < procPartFutures.size(); j++)
                procPartFutures.get(j).cancel(false);

            for (int j = curIdx; j < procIdxFutures.size(); j++)
                procIdxFutures.get(j).cancel(false);

            if (e instanceof InterruptedException)
                throw new IgniteInterruptedException((InterruptedException)e);
            else if (e.getCause() instanceof IgniteException)
                throw (IgniteException)e.getCause();
            else
                throw new IgniteException(e.getCause());
        }

        return new VisorValidateIndexesJobResult(partResults, idxResults);
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

            partRes = new ValidateIndexesPartitionResult(updateCntrBefore, partSize, isPrimary, consId, null);

            boolean enoughIssues = false;

            GridQueryProcessor qryProcessor = ignite.context().query();

            Method m;
            try {
                m = GridQueryProcessor.class.getDeclaredMethod("typeByValue", String.class,
                    CacheObjectContext.class, KeyCacheObject.class, CacheObject.class, boolean.class);
            }
            catch (NoSuchMethodException e) {
                log.error("Failed to invoke typeByValue", e);

                throw new IgniteException(e);
            }

            m.setAccessible(true);

            final boolean skipConditions = checkFirst > 0 || checkThrough > 0;
            final boolean bothSkipConditions = checkFirst > 0 && checkThrough > 0;

            long current = 0;
            long processedNumber = 0;

            while (it.hasNextX()) {
                if (enoughIssues)
                    break;

                CacheDataRow row = it.nextX();

                if (skipConditions) {
                    if (bothSkipConditions) {
                        if (processedNumber > checkFirst)
                            break;
                        else if (current++ % checkThrough > 0)
                            continue;
                        else
                            processedNumber++;
                    }
                    else {
                        if (checkFirst > 0) {
                            if (current++ > checkFirst)
                                break;
                        }
                        else {
                            if (current++ % checkThrough > 0)
                                continue;
                        }
                    }
                }

                int cacheId = row.cacheId() == 0 ? grpCtx.groupId() : row.cacheId();

                GridCacheContext cacheCtx = row.cacheId() == 0 ?
                    grpCtx.singleCacheContext() : grpCtx.shared().cacheContext(row.cacheId());

                if (cacheCtx == null)
                    throw new IgniteException("Unknown cacheId of CacheDataRow: " + cacheId);

                try {
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
                                throw new IgniteCheckedException("Key is present in CacheDataTree, but can't be found in SQL index.");
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
                catch (IllegalAccessException e) {
                    log.error("Failed to invoke typeByValue", e);

                    throw new IgniteException(e);
                }
                catch (InvocationTargetException e) {
                    Throwable target = e.getTargetException();

                    log.error("Failed to invoke typeByValue", target);

                    throw new IgniteException(target);
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

            printProgressIfNeeded();
        }

        PartitionKey partKey = new PartitionKey(grpCtx.groupId(), part.id(), grpCtx.cacheOrGroupName());

        processedPartitions.incrementAndGet();

        return Collections.singletonMap(partKey, partRes);
    }

    /**
     *
     */
    private void printProgressIfNeeded() {
        long curTs = U.currentTimeMillis();

        long lastTs = lastProgressPrintTs.get();

        if (curTs - lastTs >= 60_000 && lastProgressPrintTs.compareAndSet(lastTs, curTs)) {
            log.warning("Current progress of ValidateIndexesClosure: processed " +
                processedPartitions.get() + " of " + totalPartitions + " partitions, " +
                processedIndexes.get() + " of " + totalIndexes + " SQL indexes");
        }
    }

    /**
     * @param ctx Context.
     * @param idx Index.
     */
    private Future<Map<String, ValidateIndexesPartitionResult>> processIndexAsync(GridCacheContext ctx, Index idx) {
        return calcExecutor.submit(new Callable<Map<String, ValidateIndexesPartitionResult>>() {
            @Override public Map<String, ValidateIndexesPartitionResult> call() throws Exception {
                return processIndex(ctx, idx);
            }
        });
    }

    /**
     * @param ctx Context.
     * @param idx Index.
     */
    private Map<String, ValidateIndexesPartitionResult> processIndex(GridCacheContext ctx, Index idx) {
        Object consId = ignite.context().discovery().localNode().consistentId();

        ValidateIndexesPartitionResult idxValidationRes = new ValidateIndexesPartitionResult(
            -1, -1, true, consId, idx.getName());

        boolean enoughIssues = false;

        Cursor cursor = null;

        try {
            cursor = idx.find((Session)null, null, null);

            if (cursor == null)
                throw new IgniteCheckedException("Can't iterate through index: " + idx);
        }
        catch (Throwable t) {
            IndexValidationIssue is = new IndexValidationIssue(null, ctx.name(), idx.getName(), t);

            log.error("Find in index failed: " + is.toString());

            enoughIssues = true;
        }

        final boolean skipConditions = checkFirst > 0 || checkThrough > 0;
        final boolean bothSkipConditions = checkFirst > 0 && checkThrough > 0;

        long current = 0;
        long processedNumber = 0;

        KeyCacheObject previousKey = null;

        while (!enoughIssues) {
            KeyCacheObject h2key = null;

            try {
                try {
                    if (!cursor.next())
                        break;
                }
                catch (IllegalStateException e) {
                    throw new IgniteCheckedException("Key is present in SQL index, but is missing in corresponding " +
                        "data page. Previous successfully read key: " +
                        CacheObjectUtils.unwrapBinaryIfNeeded(ctx.cacheObjectContext(), previousKey, true, true), e);
                }

                GridH2Row h2Row = (GridH2Row)cursor.get();

                if (skipConditions) {
                    if (bothSkipConditions) {
                        if (processedNumber > checkFirst)
                            break;
                        else if (current++ % checkThrough > 0)
                            continue;
                        else
                            processedNumber++;
                    }
                    else {
                        if (checkFirst > 0) {
                            if (current++ > checkFirst)
                                break;
                        }
                        else {
                            if (current++ % checkThrough > 0)
                                continue;
                        }
                    }
                }

                h2key = h2Row.key();

                CacheDataRow cacheDataStoreRow = ctx.group().offheap().read(ctx, h2key);

                if (cacheDataStoreRow == null)
                    throw new IgniteCheckedException("Key is present in SQL index, but can't be found in CacheDataTree.");

                previousKey = h2key;
            }
            catch (Throwable t) {
                Object o = CacheObjectUtils.unwrapBinaryIfNeeded(
                    ctx.cacheObjectContext(), h2key, true, true);

                IndexValidationIssue is = new IndexValidationIssue(
                    String.valueOf(o), ctx.name(), idx.getName(), t);

                log.error("Failed to lookup key: " + is.toString());

                enoughIssues |= idxValidationRes.reportIssue(is);
            }
        }

        String uniqueIdxName = "[cache=" + ctx.name() + ", idx=" + idx.getName() + "]";

        processedIndexes.incrementAndGet();

        printProgressIfNeeded();

        return Collections.singletonMap(uniqueIdxName, idxValidationRes);
    }
}
