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
 *
 */

package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jsr166.ConcurrentLinkedDeque8;

/**
 * Keep statistic history information about run queries.
 */
public class QueryHistoryManager implements QueryHistory {
    /** History size. */
    private final int histSz;

    /** Query metrics. */
    private final ConcurrentHashMap<QueryHistoryMetricsKey, QueryHistoryMetricsAdapter> qryMetrics;

    /** Queue. */
    private final ConcurrentLinkedDeque8<QueryHistoryMetricsAdapter> evictionQueue = new ConcurrentLinkedDeque8<>();

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public QueryHistoryManager(GridKernalContext ctx) {
        histSz = ctx.config().getQueryHistoryStatisticsSize();

        qryMetrics = histSz > 0 ? new ConcurrentHashMap<>(histSz) : null;
    }

    /**
     * @param failed {@code True} if query execution failed.
     */
    public void collectMetrics(GridRunningQueryInfo runningQryInfo, boolean failed) {
        if (histSz <= 0)
            return;

        String qry = runningQryInfo.query();
        String schema = runningQryInfo.schemaName();
        boolean loc = runningQryInfo.local();
        long startTime = runningQryInfo.startTime();
        long duration = U.currentTimeMillis() - startTime;

        // Do not collect metrics for EXPLAIN queries.
        if (explain(qry))
            return;

        boolean metricsFull = qryMetrics.size() >= histSz;

        QueryHistoryMetricsAdapter m = new QueryHistoryMetricsAdapter(qry, schema, loc, startTime, duration, failed);

        QueryHistoryMetricsAdapter mergedMetrics = qryMetrics.merge(m.key(), m, QueryHistoryMetricsAdapter::aggregateWithNew);

        if (touch(mergedMetrics) && metricsFull)
            shrink();
    }

    /**
     * @param entry Entry Which was updated
     * @return {@code true} In case entry is new and has been added, {@code false} otherwise.
     */
    private boolean touch(QueryHistoryMetricsAdapter entry) {
        ConcurrentLinkedDeque8.Node<QueryHistoryMetricsAdapter> node = entry.link();

        // Entry has not been enqueued yet.
        if (node == null) {
            node = evictionQueue.offerLastx(entry);

            if (!entry.setLinkIfAbsent(node)) {
                // Was concurrently added, need to clear it from queue.
                removeLink(node);

                return false;
            }
            else if (qryMetrics.get(entry.key()) != entry) {
                // Was concurrently evicted, need to clear it from queue.
                removeLink(node);

                return false;
            }

            return true;
        }
        else if (removeLink(node)) {
            // Move node to tail.
            ConcurrentLinkedDeque8.Node<QueryHistoryMetricsAdapter> newNode = evictionQueue.offerLastx(entry);

            if (!entry.replaceLink(node, newNode))
                // Was concurrently added, need to clear it from queue.
                removeLink(newNode);
        }

        // Entry is already in queue.
        return false;
    }

    /**
     * Tries to remove one item from queue.
     */
    private void shrink() {
        while (true) {
            QueryHistoryMetricsAdapter entry = evictionQueue.poll();

            if (entry == null)
                return;

            //Metrics has been changed if we can't remove metric entry.
            // In this case eviction queue already offered by the entry and we don't put it back. Just try to do new
            // attempt to remove oldest entry.
            if (qryMetrics.remove(entry.key(), entry))
                return;
        }
    }

    /**
     * @param node Node wchi should be unlinked from eviction queue.
     * @return {@code true} If node was unlinked.
     */
    private boolean removeLink(ConcurrentLinkedDeque8.Node<QueryHistoryMetricsAdapter> node) {
        return evictionQueue.unlinkx(node);
    }

    /**
     * @param qry Textual query representation.
     * @return {@code true} in case query is explain.
     */
    private boolean explain(String qry) {
        int off = 0;
        int len = qry.length();

        while (off < len && Character.isWhitespace(qry.charAt(off)))
            off++;

        return qry.regionMatches(true, off, "EXPLAIN", 0, 7);
    }

    /** {@inheritDoc} */
    @Override public Collection<QueryHistoryMetricsAdapter> queryHistoryMetrics() {
        if (histSz <= 0)
            return Collections.emptyList();

        Object[] metrics = evictionQueue.toArray();

        int cnt = metrics.length;
        int firstIdx = Math.min(0, Math.max(0, cnt - histSz));
        int resCnt = Math.min(histSz, cnt);

        QueryHistoryMetricsAdapter[] latestMetrics = new QueryHistoryMetricsAdapter[resCnt];

        for (int i = 0; i < resCnt; i++) {
            QueryHistoryMetricsAdapter item = (QueryHistoryMetricsAdapter)metrics[firstIdx + i];

            latestMetrics[cnt - 1 - i] = item;
        }

        return Arrays.asList(latestMetrics);
    }

    /** {@inheritDoc} */
    @Override public void resetQueryHistoryMetrics() {
        if (histSz <= 0)
            return;

        evictionQueue.clear();

        qryMetrics.clear();
    }
}
