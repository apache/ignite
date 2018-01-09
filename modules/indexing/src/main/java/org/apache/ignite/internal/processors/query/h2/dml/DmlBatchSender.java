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

package org.apache.ignite.internal.processors.query.h2.dml;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.createJdbcSqlException;

/**
 * Batch sender class.
 */
public class DmlBatchSender {
    /** Cache context. */
    private final GridCacheContext cctx;

    /** Batch size. */
    private final int size;

    /** Batches. */
    private final Map<UUID, Map<Object, IgniteBiTuple<Integer, EntryProcessor<Object, Object, Boolean>>>> batches =
        new HashMap<>();

    /** Result count. */
    private long updateCnt;

    /** Failed keys. */
    private List<Object> failedKeys;

    /** Exception. */
    private SQLException err;

    /** Per row updates counter */
    private int[] cntPerRow;

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     * @param size Batch.
     * @param qryNum Number of queries.
     */
    public DmlBatchSender(GridCacheContext cctx, int size, int qryNum) {
        this.cctx = cctx;
        this.size = size;
        cntPerRow = new int[qryNum];
    }

    /**
     * Add entry to batch.
     *
     * @param key Key.
     * @param proc Processor.
     * @param rowNum Row number.
     * @throws IgniteCheckedException If failed.
     */
    public void add(Object key, EntryProcessor<Object, Object, Boolean> proc, int rowNum)
        throws IgniteCheckedException {
        ClusterNode node = cctx.affinity().primaryByKey(key, AffinityTopologyVersion.NONE);

        if (node == null)
            throw new IgniteCheckedException("Failed to map key to node.");

        assert rowNum < cntPerRow.length;

        UUID nodeId = node.id();

        Map<Object, IgniteBiTuple<Integer, EntryProcessor<Object, Object, Boolean>>> batch = batches.get(nodeId);

        if (batch == null) {
            batch = new HashMap<>();

            batches.put(nodeId, batch);
        }

        if (batch.containsKey(key)) { // Force cache update if duplicates found.
            sendBatch(batch);

            batch.clear();
        }

        batch.put(key, new IgniteBiTuple<>(rowNum, proc));

        if (batch.size() >= size) {
            sendBatch(batch);

            batch.clear();
        }
    }

    /**
     * Flush any remaining entries.
     */
    public void flush() {
        for (Map<Object, IgniteBiTuple<Integer, EntryProcessor<Object, Object, Boolean>>> batch : batches.values()) {
            if (!batch.isEmpty()) {
                sendBatch(batch);

                batch.clear();
            }
        }
    }

    /**
     * @return Update count.
     */
    public long updateCount() {
        return updateCnt;
    }

    /**
     * @return Failed keys.
     */
    public List<Object> failedKeys() {
        return failedKeys != null ? failedKeys : Collections.emptyList();
    }

    /**
     * @return Error.
     */
    public SQLException error() {
        return err;
    }

    /**
     * Returns per row updates counter as array.
     *
     * @return Per row updates counter as array.
     */
    public int[] perRowCounterAsArray() {
        return cntPerRow;
    }

    /**
     * Sets row as failed.
     *
     * @param rowNum Row number.
     */
    public void setFailed(int rowNum) {
        cntPerRow[rowNum] = Statement.EXECUTE_FAILED;
    }

    /**
     * Send the batch.
     *
     * @param batch Batch.
     */
    private void sendBatch(Map<Object, IgniteBiTuple<Integer, EntryProcessor<Object, Object, Boolean>>> batch) {
        DmlPageProcessingResult pageRes = processPage(cctx, batch);

        updateCnt += pageRes.count();

        if (failedKeys == null)
            failedKeys = new ArrayList<>();

        failedKeys.addAll(F.asList(pageRes.errorKeys()));

        if (pageRes.error() != null) {
            if (err == null)
                err = pageRes.error();
            else
                err.setNextException(pageRes.error());
        }
    }

    /**
     * Execute given entry processors and collect errors, if any.
     * @param cctx Cache context.
     * @param rows Rows to process.
     * @return Triple [number of rows actually changed; keys that failed to update (duplicates or concurrently
     *     updated ones); chain of exceptions for all keys whose processing resulted in error, or null for no errors].
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private DmlPageProcessingResult processPage(GridCacheContext cctx,
        Map<Object, IgniteBiTuple<Integer, EntryProcessor<Object, Object, Boolean>>> rows) {
        Map<Object, EntryProcessor<Object, Object, Boolean>> keysAndProcs = new HashMap<>();

        for (Map.Entry<Object, IgniteBiTuple<Integer, EntryProcessor<Object, Object, Boolean>>> entry : rows.entrySet())
            keysAndProcs.put(entry.getKey(), entry.getValue().get2());

        Map<Object, EntryProcessorResult<Boolean>> res;

        try {
            res = cctx.cache().invokeAll(keysAndProcs);
        }
        catch (IgniteCheckedException e) {
            for (IgniteBiTuple<Integer, EntryProcessor<Object, Object, Boolean>> val : rows.values()) {
                Integer rowNum = val.get1();

                assert rowNum != null;

                cntPerRow[rowNum] = Statement.EXECUTE_FAILED;
            }

            return new DmlPageProcessingResult(0, null,
                new SQLException(e.getMessage(), SqlStateCode.INTERNAL_ERROR, IgniteQueryErrorCode.UNKNOWN, e));
        }

        if (F.isEmpty(res)) {
            countAllRows(rows);

            return new DmlPageProcessingResult(rows.size(), null, null);
        }

        DmlPageProcessingErrorResult splitRes = splitErrors(res, rows);

        int keysCnt = splitRes.errorKeys().length;

        return new DmlPageProcessingResult(rows.size() - keysCnt - splitRes.errorCount(), splitRes.errorKeys(),
            splitRes.error());
    }

    /**
     * Process errors of entry processor - split the keys into duplicated/concurrently modified and those whose
     * processing yielded an exception.
     *
     * @param res Result of {@link GridCacheAdapter#invokeAll)}
     * @return pair [array of duplicated/concurrently modified keys, SQL exception for erroneous keys] (exception is
     * null if all keys are duplicates/concurrently modified ones).
     */
    private DmlPageProcessingErrorResult splitErrors(Map<Object, EntryProcessorResult<Boolean>> res,
        Map<Object, IgniteBiTuple<Integer, EntryProcessor<Object, Object, Boolean>>> rows) {
        Set<Object> errKeys = new LinkedHashSet<>(res.keySet());

        countAllRows(rows);

        SQLException currSqlEx = null;

        SQLException firstSqlEx = null;

        int errors = 0;

        // Let's form a chain of SQL exceptions
        for (Map.Entry<Object, EntryProcessorResult<Boolean>> e : res.entrySet()) {
            try {
                e.getValue().get();
            }
            catch (EntryProcessorException ex) {
                SQLException next = createJdbcSqlException("Failed to process key '" + e.getKey() + '\'',
                    IgniteQueryErrorCode.ENTRY_PROCESSING);

                next.initCause(ex);

                if (currSqlEx != null)
                    currSqlEx.setNextException(next);
                else
                    firstSqlEx = next;

                currSqlEx = next;

                errKeys.remove(e.getKey());

                errors++;
            }
            finally {
                Object key = e.getKey();

                Integer rowNum = rows.get(key).get1();

                assert rowNum != null;

                cntPerRow[rowNum] = Statement.EXECUTE_FAILED;
            }
        }

        return new DmlPageProcessingErrorResult(errKeys.toArray(), firstSqlEx, errors);
    }

    /**
     * Updates counters as if all rows were successfully processed.
     *
     * @param rows Rows.
     */
    private void countAllRows(Map<Object, IgniteBiTuple<Integer, EntryProcessor<Object, Object, Boolean>>> rows) {
        for (IgniteBiTuple<Integer, EntryProcessor<Object, Object, Boolean>> val : rows.values()) {
            Integer rowNum = val.get1();

            assert rowNum != null;

            if (cntPerRow[rowNum] > -1)
                cntPerRow[rowNum]++;
        }
    }
}
