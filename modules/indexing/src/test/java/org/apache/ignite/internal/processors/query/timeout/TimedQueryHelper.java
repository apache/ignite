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

package org.apache.ignite.internal.processors.query.timeout;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;

import static org.apache.ignite.testframework.junits.common.GridCommonAbstractTest.awaitCacheOnClient;

/**
 * This class helps to prepare a query which will run for a specific amount of time
 * and able to be cancelled by timeout.
 * Some tricks is needed because internally (H2) a query is checked for timeout after retrieving every N rows.
 */
public class TimedQueryHelper {
    /** Row count. */
    private static final int ROW_COUNT = 250;

    /** Execution time. */
    private final long executionTime;

    /** Cache name. */
    private final String cacheName;

    /**
     * @param executionTime Query execution time.
     * @param cacheName Cache name.
     */
    public TimedQueryHelper(long executionTime, String cacheName) {
        assert executionTime >= ROW_COUNT;

        this.executionTime = executionTime;
        this.cacheName = cacheName;
    }

    /** */
    public void createCache(Ignite ign) {
        IgniteCache<Object, Object> cache = ign.createCache(new CacheConfiguration<>(cacheName)
            .setSqlSchema("PUBLIC")
            .setQueryEntities(Collections.singleton(
                new QueryEntity(Integer.class, Integer.class).setTableName(cacheName)))
            .setSqlFunctionClasses(TimedQueryHelper.class));

        F.view(G.allGrids(), ignite -> ignite.cluster().localNode().isClient())
            .forEach(ignite -> awaitCacheOnClient(ignite, cacheName));

        Map<Integer, Integer> entries = IntStream.range(0, ROW_COUNT).boxed()
            .collect(Collectors.toMap(Function.identity(), Function.identity()));

        cache.putAll(entries);
    }

    /** */
    public String buildTimedQuery() {
        long rowTimeout = executionTime / ROW_COUNT;

        return "select longProcess(_val, " + rowTimeout + ") from " + cacheName;
    }

    /** */
    public String buildTimedUpdateQuery() {
        long rowTimeout = executionTime / ROW_COUNT;

        return "update " + cacheName + " set _val = longProcess(_val, " + rowTimeout + ")";
    }

    /** */
    @QuerySqlFunction
    public static int longProcess(int i, long millis) {
        try {
            Thread.sleep(millis);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return i;
    }
}
