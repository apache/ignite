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

package org.apache.ignite.internal.processors.security.cache.closure;

import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractCacheOperationRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteRunnable;

import static org.apache.ignite.Ignition.localIgnite;

/**
 * The base class for tests that check continuous queries {@link SecurityContext} on a remote node.
 */
public class AbstractContinuousQueryRemoteSecurityContextCheckTest extends
    AbstractCacheOperationRemoteSecurityContextCheckTest {
    /** Server node to change cache state. */
    private static final String SRV = "srv";

    /** Cache index. */
    private static final AtomicInteger CACHE_INDEX = new AtomicInteger();

    /** Open continuous query operation. */
    protected static final String OPERATION_OPEN_CQ = "open_cq";

    /** Init query, filter or transform operation. */
    protected static final String OPERATION_CQ_COMPONENT = "cq_component";

    /** Preidacte for inital query tests. */
    protected static final IgniteBiPredicate<Integer, Integer> INITIAL_QUERY_FILTER = (k, v) -> {
        VERIFIER.register(OPERATION_CQ_COMPONENT);

        return true;
    };

    /** Remote filter. */
    protected static final CacheEntryEventSerializableFilter<Integer, Integer> RMT_FILTER = e -> {
        VERIFIER.register(OPERATION_CQ_COMPONENT);

        return true;
    };

    /** {@inheritDoc} */
    @Override protected CacheConfiguration[] getCacheConfigurations() {
        return new CacheConfiguration[] {};
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        IgniteEx srv = startGridAllowAll(SRV);

        startGridAllowAll(SRV_INITIATOR);

        startClientAllowAll(CLNT_INITIATOR);

        startGridAllowAll(SRV_RUN);

        startClientAllowAll(CLNT_RUN);

        startGridAllowAll(SRV_CHECK);

        srv.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void runAndCheck(IgniteRunnable op) {
        initiators().forEach(initiator -> {
            VERIFIER.initiator(initiator)
                .expect(SRV_RUN, OPERATION_OPEN_CQ, 1)
                .expect(CLNT_RUN, OPERATION_OPEN_CQ, 1)
                .expect(SRV_CHECK, OPERATION_CQ_COMPONENT, 2);

            compute(initiator, nodesToRunIds()).broadcast(op);

            VERIFIER.checkResult();
        });
    }

    /**
     * Opens query cursor.
     *
     * @param q {@link Query}.
     * @param init True if needing put data to a cache before openning a cursor.
     */
    protected void executeQuery(Query<Cache.Entry<Integer, Integer>> q, boolean init) {
        Ignite ignite = localIgnite();

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(
            new CacheConfiguration<Integer, Integer>()
                .setName(CACHE_NAME + CACHE_INDEX.incrementAndGet())
                .setCacheMode(CacheMode.PARTITIONED)
        );

        if (init)
            cache.put(primaryKey(grid(SRV_CHECK), cache.getName()), 100);

        try (QueryCursor<Cache.Entry<Integer, Integer>> cur = cache.query(q)) {
            if (!init)
                cache.put(primaryKey(grid(SRV_CHECK), cache.getName()), 100);

            cur.getAll();
        }
    }
}
