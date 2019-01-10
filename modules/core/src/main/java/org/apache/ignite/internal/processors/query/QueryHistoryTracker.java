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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jsr166.ConcurrentLinkedDeque8;

/**
 *
 */
class QueryHistoryTracker {
    /** Query metrics. */
    private final ConcurrentHashMap<QueryHistoryMetricsKey, QueryHistoryMetrics> qryMetrics;

    /** Queue. */
    private final ConcurrentLinkedDeque8<QueryHistoryMetrics> evictionQueue = new ConcurrentLinkedDeque8<>();

    /** History size. */
    private final int histSz;

    /**
     * @param histSz History size.
     */
    QueryHistoryTracker(int histSz) {
        this.histSz = histSz;

        qryMetrics = histSz > 0 ? new ConcurrentHashMap<>(histSz) : null;
    }

    /**
     * @param failed {@code True} if query execution failed.
     */
    void collectMetrics(GridRunningQueryInfo runningQryInfo, boolean failed) {
        if (histSz <= 0)
            return;

        String qry = runningQryInfo.query();
        String schema = runningQryInfo.schemaName();
        boolean loc = runningQryInfo.local();
        long startTime = runningQryInfo.startTime();
        long duration = U.currentTimeMillis() - startTime;

        QueryHistoryMetrics m = new QueryHistoryMetrics(qry, schema, loc, startTime, duration, failed);

        QueryHistoryMetrics mergedMetrics = qryMetrics.merge(m.key(), m, QueryHistoryMetrics::aggregateWithNew);

        if (touch(mergedMetrics) && qryMetrics.size() > histSz)
            shrink();
    }

    /**
     * @param entry Entry Which was updated
     * @return {@code true} In case entry is new and has been added, {@code false} otherwise.
     */
    private boolean touch(QueryHistoryMetrics entry) {
        ConcurrentLinkedDeque8.Node<QueryHistoryMetrics> node = entry.link();

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
            ConcurrentLinkedDeque8.Node<QueryHistoryMetrics> newNode = evictionQueue.offerLastx(entry);

            if (!entry.replaceLink(node, newNode)) {
                // Was concurrently added, need to clear it from queue.
                removeLink(newNode);

                return false;
            }
            else if (qryMetrics.get(entry.key()) != entry) {
                // Was concurrently evicted, need to clear it from queue.
                removeLink(node);

                return false;
            }
        }

        // Entry is already in queue.
        return false;
    }

    /**
     * Tries to remove one item from queue.
     */
    private void shrink() {
        while (true) {
            QueryHistoryMetrics entry = evictionQueue.poll();

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
    private boolean removeLink(ConcurrentLinkedDeque8.Node<QueryHistoryMetrics> node) {
        return evictionQueue.unlinkx(node);
    }

    /**
     * Gets query history statistics. Size of history could be configured via {@link
     * IgniteConfiguration#setQueryHistoryStatisticsSize(int)}
     *
     * @return Queries history statistics aggregated by query text, schema and local flag.
     */
    Collection<QueryHistoryMetrics> queryHistoryMetrics() {
        if (histSz <= 0)
            return Collections.emptyList();

        ArrayList<QueryHistoryMetrics> latestMetrics = new ArrayList<>(histSz);

        // We need filter possible duplicates which can appear due to concurrent update.
        Set<QueryHistoryMetricsKey> addedKeysSet = new HashSet<>(histSz);

        for (QueryHistoryMetrics metrics : evictionQueue) {
            //Skip not fully applied changes and duplicates
            if (metrics.link() != null && addedKeysSet.add(metrics.key()))
                latestMetrics.add(metrics);

            if (latestMetrics.size() == histSz)
                break;

        }

        return latestMetrics;
    }
}
