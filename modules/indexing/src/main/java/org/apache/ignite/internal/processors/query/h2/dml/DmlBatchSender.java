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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteClusterReadOnlyException;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.odbc.SqlStateCode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;

import static org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode.createJdbcSqlException;

/**
 * Batch sender class.
 */
public class DmlBatchSender {
    /** Comparator. */
    private static final BatchEntryComparator COMP = new BatchEntryComparator();

    /** Cache context. */
    private final GridCacheContext cctx;

    /** Batch size. */
    private final int size;

    /** Batches. */
    private final Map<UUID, Batch> batches = new HashMap<>();

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
        assert key != null;
        assert proc != null;
        assert rowNum < cntPerRow.length;

        ClusterNode node = primaryNodeByKey(key);

        UUID nodeId = node.id();

        Batch batch = batches.get(nodeId);

        if (batch == null) {
            batch = new Batch();

            batches.put(nodeId, batch);
        }

        if (batch.containsKey(key)) { // Force cache update if duplicates found.
            sendBatch(batch);
        }

        batch.put(key, rowNum, proc);

        if (batch.size() >= size)
            sendBatch(batch);
    }

    /**
     * @param key Key.
     * @return Primary node for given key.
     * @throws IgniteCheckedException If primary node is not found.
     */
    public ClusterNode primaryNodeByKey(Object key) throws IgniteCheckedException {
        ClusterNode node = cctx.affinity().primaryByKey(key, AffinityTopologyVersion.NONE);

        if (node == null)
            throw new IgniteCheckedException("Failed to map key to node.");

        return node;
    }

    /**
     * Flush any remaining entries.
     */
    public void flush() {
        for (Batch batch : batches.values()) {
            if (!batch.isEmpty())
                sendBatch(batch);
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
    private void sendBatch(Batch batch) {
        DmlPageProcessingResult pageRes = processPage(cctx, batch);

        batch.clear();

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
     * @param batch Rows to process.
     * @return Triple [number of rows actually changed; keys that failed to update (duplicates or concurrently
     *     updated ones); chain of exceptions for all keys whose processing resulted in error, or null for no errors].
     */
    @SuppressWarnings({"unchecked"})
    private DmlPageProcessingResult processPage(GridCacheContext cctx, Batch batch) {
        Map<Object, EntryProcessorResult<Boolean>> res;

        try {
            res = cctx.cache().invokeAll(batch.rowProcessors());
        }
        catch (IgniteCheckedException e) {
            for (Integer rowNum : batch.rowNumbers().values()) {
                assert rowNum != null;

                cntPerRow[rowNum] = Statement.EXECUTE_FAILED;
            }

            if (X.hasCause(e, IgniteClusterReadOnlyException.class)) {
                SQLException sqlEx = new SQLException(
                    e.getMessage(),
                    SqlStateCode.CLUSTER_READ_ONLY_MODE_ENABLED,
                    IgniteQueryErrorCode.CLUSTER_READ_ONLY_MODE_ENABLED,
                    e
                );

                return new DmlPageProcessingResult(0, null, sqlEx);
            }

            return new DmlPageProcessingResult(0, null,
                new SQLException(e.getMessage(), SqlStateCode.INTERNAL_ERROR, IgniteQueryErrorCode.UNKNOWN, e));
        }

        if (F.isEmpty(res)) {
            countAllRows(batch.rowNumbers().values());

            return new DmlPageProcessingResult(batch.size(), null, null);
        }

        DmlPageProcessingErrorResult splitRes = splitErrors(res, batch);

        int keysCnt = splitRes.errorKeys().length;

        return new DmlPageProcessingResult(batch.size() - keysCnt - splitRes.errorCount(), splitRes.errorKeys(),
            splitRes.error());
    }

    /**
     * Process errors of entry processor - split the keys into duplicated/concurrently modified and those whose
     * processing yielded an exception.
     *
     * @param res Result of {@link GridCacheAdapter#invokeAll)}
     * @param batch Batch.
     * @return pair [array of duplicated/concurrently modified keys, SQL exception for erroneous keys] (exception is
     * null if all keys are duplicates/concurrently modified ones).
     */
    private DmlPageProcessingErrorResult splitErrors(Map<Object, EntryProcessorResult<Boolean>> res, Batch batch) {
        Set<Object> errKeys = new LinkedHashSet<>(res.keySet());

        countAllRows(batch.rowNumbers().values());

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

                Integer rowNum = batch.rowNumbers().get(key);

                assert rowNum != null;

                cntPerRow[rowNum] = Statement.EXECUTE_FAILED;
            }
        }

        return new DmlPageProcessingErrorResult(errKeys.toArray(), firstSqlEx, errors);
    }

    /**
     * Updates counters as if all rowNums were successfully processed.
     *
     * @param rowNums Rows.
     */
    private void countAllRows(Collection<Integer> rowNums) {
        for (Integer rowNum : rowNums) {
            assert rowNum != null;

            if (cntPerRow[rowNum] > -1)
                cntPerRow[rowNum]++;
        }
    }

    /**
     * Batch for update.
     */
    private static class Batch {
        /** Map from keys to row numbers. */
        private Map<Object, Integer> rowNums = new HashMap<>();

        /** Map from keys to entry processors. */
        private Map<Object, EntryProcessor<Object, Object, Boolean>> rowProcs = new TreeMap<>(COMP);

        /**
         * Checks if batch contains key.
         *
         * @param key Key.
         * @return {@code True} if contains.
         */
        public boolean containsKey(Object key) {
            boolean res = rowNums.containsKey(key);

            assert res == rowProcs.containsKey(key);

            return res;
        }

        /**
         * Returns batch size.
         *
         * @return Batch size.
         */
        public int size() {
            int res = rowNums.size();

            assert res == rowProcs.size();

            return res;
        }

        /**
         * Adds row to batch.
         *
         * @param key Key.
         * @param rowNum Row number.
         * @param proc Entry processor.
         * @return {@code True} if there was an entry associated with the given key.
         */
        public boolean put(Object key, Integer rowNum, EntryProcessor<Object, Object, Boolean> proc) {
            Integer prevNum = rowNums.put(key, rowNum);
            EntryProcessor prevProc = rowProcs.put(key, proc);

            assert (prevNum == null) == (prevProc == null);

            return prevNum != null;
        }

        /**
         * Clears batch.
         */
        public void clear() {
            assert rowNums.size() == rowProcs.size();

            rowNums.clear();
            rowProcs.clear();
        }

        /**
         * Checks if batch is empty.
         *
         * @return {@code True} if empty.
         */
        public boolean isEmpty() {
            assert rowNums.size() == rowProcs.size();

            return rowNums.isEmpty();
        }

        /**
         * Row numbers map getter.
         *
         * @return Row numbers map.
         */
        public Map<Object, Integer> rowNumbers() {
            return rowNums;
        }

        /**
         * Row processors map getter.
         *
         * @return Row processors map.
         */
        public Map<Object, EntryProcessor<Object, Object, Boolean>> rowProcessors() {
            return rowProcs;
        }
    }

    /**
     * Batch entries comparator.
     */
    private static final class BatchEntryComparator implements Comparator<Object> {
        /** {@inheritDoc} */
        @Override public int compare(Object first, Object second) {
            // We assume that only simple types or BinaryObjectImpl are possible. The latter comes from the fact
            // that we use BinaryObjectBuilder which produces only on-heap binary objects.
            return BinaryObjectImpl.compareForDml(first, second);
        }
    }
}
