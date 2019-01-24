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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.configuration.IgniteConfiguration;
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
        long duration = System.currentTimeMillis() - startTime;

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

            if (!entry.replaceLink(null, node)) {
                // Was concurrently added, need to clear it from queue.
                removeLink(node);

                return false;
            }

            if (node.item() == null) {
                // Was concurrently shrinked.
                entry.replaceLink(node, null);

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

            // Metrics has been changed if we can't remove metric entry.
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
     * Gets SQL query history. Size of history could be configured via {@link
     * IgniteConfiguration#setSqlQueryHistorySize(int)}
     *
     * @return SQL queries history aggregated by query text, schema and local flag.
     */
    Map<QueryHistoryMetricsKey, QueryHistoryMetrics> queryHistoryMetrics() {
        if (histSz <= 0)
            return Collections.emptyMap();

        return new HashMap<>(qryMetrics);
    }
}
