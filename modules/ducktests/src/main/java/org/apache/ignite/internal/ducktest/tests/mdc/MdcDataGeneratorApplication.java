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

package org.apache.ignite.internal.ducktest.tests.mdc;

import java.util.Map;
import java.util.TreeMap;
import java.util.function.IntFunction;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.ducktest.tests.dto.IndexedDataRecord;

/**
 * Populates the MDC cache with a deterministic data set: keys in {@code [from, to)},
 * values {@code IndexedDataRecord(key)} (or the key itself in {@code sqlMode}).
 * Run it to completion before enabling the network partition.
 */
public class MdcDataGeneratorApplication extends MdcCacheAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jNode) throws IgniteInterruptedCheckedException {
        int from = jNode.path("from").asInt(0);
        int to = jNode.path("to").asInt(10_000);
        int batchSize = jNode.path("batchSize").asInt(1_024);
        boolean sqlMode = jNode.path("sqlMode").asBoolean(false);

        markInitialized();
        waitForActivation();

        log.info("Data generation started [dc=" + dcId() + ", sqlMode=" + sqlMode +
            ", from=" + from + ", to=" + to + "]");

        if (sqlMode)
            load(mdcSqlCache(jNode), from, to, batchSize, i -> i);
        else
            load(mdcCache(jNode), from, to, batchSize, IndexedDataRecord::new);

        log.info("Data generation finished [dc=" + dcId() + ", entries=" + (to - from) + "]");

        markFinished();
    }

    /**
     * Streams keys {@code [from, to)} into the cache in {@code putAll} batches, deriving
     * each value from its key.
     */
    private <V> void load(IgniteCache<Integer, V> cache, int from, int to, int batchSize, IntFunction<V> valFn) {
        Map<Integer, V> batch = new TreeMap<>();

        for (int i = from; i < to && !terminated(); i++) {
            batch.put(i, valFn.apply(i));

            if (batch.size() >= batchSize) {
                cache.putAll(batch);
                batch.clear();
            }
        }

        if (!batch.isEmpty() && !terminated())
            cache.putAll(batch);
    }
}
