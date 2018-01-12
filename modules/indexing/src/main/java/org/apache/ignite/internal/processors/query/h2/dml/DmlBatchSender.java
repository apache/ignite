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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.util.typedef.F;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

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
    private final Map<UUID, Map<Object, EntryProcessor<Object, Object, Boolean>>> batches = new HashMap<>();

    /** Result count. */
    private long updateCnt;

    /** Failed keys. */
    private List<Object> failedKeys;

    /** Exception. */
    private SQLException err;

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     * @param size Batch.
     */
    public DmlBatchSender(GridCacheContext cctx, int size) {
        this.cctx = cctx;
        this.size = size;
    }

    /**
     * Add entry to batch.
     *
     * @param key Key.
     * @param proc Processor.
     */
    public void add(Object key, EntryProcessor<Object, Object, Boolean> proc) throws IgniteCheckedException {
        ClusterNode node = cctx.affinity().primaryByKey(key, AffinityTopologyVersion.NONE);

        if (node == null)
            throw new IgniteCheckedException("Failed to map key to node.");

        UUID nodeId = node.id();

        Map<Object, EntryProcessor<Object, Object, Boolean>> batch = batches.get(nodeId);

        if (batch == null) {
            batch = new HashMap<>();

            batches.put(nodeId, batch);
        }

        batch.put(key, proc);

        if (batch.size() >= size) {
            sendBatch(batch);

            batch.clear();
        }
    }

    /**
     * Flush any remaining entries.
     *
     * @throws IgniteCheckedException If failed.
     */
    public void flush() throws IgniteCheckedException {
        for (Map<Object, EntryProcessor<Object, Object, Boolean>> batch : batches.values()) {
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
     * Send the batch.
     *
     * @param batch Batch.
     * @throws IgniteCheckedException If failed.
     */
    private void sendBatch(Map<Object, EntryProcessor<Object, Object, Boolean>> batch)
        throws IgniteCheckedException {
        DmlPageProcessingResult pageRes = processPage(cctx, batch);

        updateCnt += pageRes.count();

        if (failedKeys == null)
            failedKeys = new ArrayList<>();

        failedKeys.addAll(F.asList(pageRes.errorKeys()));

        if (pageRes.error() != null) {
            if (err == null)
                err = error();
            else
                err.setNextException(error());
        }
    }

    /**
     * Execute given entry processors and collect errors, if any.
     * @param cctx Cache context.
     * @param rows Rows to process.
     * @return Triple [number of rows actually changed; keys that failed to update (duplicates or concurrently
     *     updated ones); chain of exceptions for all keys whose processing resulted in error, or null for no errors].
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions"})
    private static DmlPageProcessingResult processPage(GridCacheContext cctx,
        Map<Object, EntryProcessor<Object, Object, Boolean>> rows) throws IgniteCheckedException {
        Map<Object, EntryProcessorResult<Boolean>> res = cctx.cache().invokeAll(rows);

        if (F.isEmpty(res))
            return new DmlPageProcessingResult(rows.size(), null, null);

        DmlPageProcessingErrorResult splitRes = splitErrors(res);

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
    private static DmlPageProcessingErrorResult splitErrors(Map<Object, EntryProcessorResult<Boolean>> res) {
        Set<Object> errKeys = new LinkedHashSet<>(res.keySet());

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
        }

        return new DmlPageProcessingErrorResult(errKeys.toArray(), firstSqlEx, errors);
    }
}
