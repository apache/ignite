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

package org.apache.ignite.internal.benchmarks.jmh.cache;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import javax.cache.Cache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.benchmarks.model.IntValue;
import org.openjdk.jmh.annotations.Benchmark;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.eq;

/** Scan and index queries; keys, values and the query objects are created in setup. */
@SuppressWarnings("unchecked")
public class JmhCacheQueryAllocationBenchmark extends JmhCacheAllocationAbstractBenchmark {
    /** Index queries created in setup, each for one value. */
    private static final int IDX_QUERIES = 1024;

    /** Scan query of each partition. */
    private ScanQuery<Integer, IntValue>[] scans;

    /** Index queries of values spread over the keys. */
    private IndexQuery<Integer, IntValue>[] idxQueries;

    /** {@inheritDoc} */
    @Override public void setup() throws Exception {
        super.setup();

        populateIntValues();

        scans = new ScanQuery[node.affinity(cache.getName()).partitions()];

        for (int p = 0; p < scans.length; p++)
            scans[p] = new ScanQuery<>(p);

        idxQueries = new IndexQuery[IDX_QUERIES];

        for (int i = 0; i < IDX_QUERIES; i++) {
            idxQueries[i] = new IndexQuery<Integer, IntValue>(IntValue.class)
                .setCriteria(eq("id", i * (CNT / IDX_QUERIES)));
        }
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        return super.cacheConfiguration()
            .setQueryEntities(List.of(new QueryEntity(Integer.class.getName(), IntValue.class.getName())
                .addQueryField("id", Integer.class.getName(), null)
                .setIndexes(List.of(new QueryIndex("id")))));
    }

    /** @return Number of entries in one partition, about a hundred, read with a scan query. */
    @Benchmark
    public int scanQuery() {
        return read(cache.query(scans[ThreadLocalRandom.current().nextInt(scans.length)]));
    }

    /** @return Number of entries an index query finds by the value of the indexed field: one. */
    @Benchmark
    public int indexQuery() {
        return read(cache.query(idxQueries[ThreadLocalRandom.current().nextInt(idxQueries.length)]));
    }

    /** @return Number of entries the cursor returns; closes it. */
    private static int read(QueryCursor<Cache.Entry<Integer, IntValue>> cur) {
        int cnt = 0;

        try (cur) {
            for (Cache.Entry<Integer, IntValue> ignored : cur)
                cnt++;
        }

        return cnt;
    }
}
