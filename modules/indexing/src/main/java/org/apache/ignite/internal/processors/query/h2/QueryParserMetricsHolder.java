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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;

/**
 * Metric holder for metrics of query parser.
 */
public class QueryParserMetricsHolder {
    /** Query parser metric group name. */
    static final String QUERY_PARSER_METRIC_GROUP_NAME = "sql.parser.cache";

    /** Query cache hits counter. */
    private final LongAdderMetric qryCacheHits;

    /** Query cache misses counter. */
    private final LongAdderMetric qryCacheMisses;

    /**
     * Create metrics holder with given metric manager.
     *
     * @param metricMgr Metric manager.
     */
    public QueryParserMetricsHolder(GridMetricManager metricMgr) {
        MetricRegistry registry = metricMgr.registry(QUERY_PARSER_METRIC_GROUP_NAME);

        qryCacheHits = registry.longAdderMetric("hits", "Count of hits for queries cache");
        qryCacheMisses = registry.longAdderMetric("misses", "Count of misses for queries cache");
    }

    /**
     * Increment cache hits counter. Should be called when query found in cache by its descriptor.
     */
    public void countCacheHit() {
        qryCacheHits.increment();
    }

    /**
     * Increment cache misses counter. Should be called when query not found in cache.
     */
    public void countCacheMiss() {
        qryCacheMisses.increment();
    }
}
