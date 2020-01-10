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

import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractCacheOperationRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.lang.IgniteBiPredicate;

import static org.apache.ignite.Ignition.localIgnite;

/**
 * The base class for tests that check continuous queries {@link SecurityContext} on a remote node.
 */
public class AbstractContinuousQueryRemoteSecurityContextCheckTest extends
    AbstractCacheOperationRemoteSecurityContextCheckTest {
    /** Server node to change cache state. */
    private static final String SRV = "srv";

    /** Preidacte for inital query tests. */
    protected static final IgniteBiPredicate<Integer, Integer> INITIAL_QUERY_FILTER = (k, v) -> {
        VERIFIER.register();

        return true;
    };

    /** Remote filter. */
    protected static final CacheEntryEventSerializableFilter<Integer, Integer> RMT_FILTER = e -> {
        VERIFIER.register();

        return true;
    };

    /** {@inheritDoc} */
    @Override protected CacheConfiguration[] getCacheConfigurations() {
        return new CacheConfiguration[] {
            new CacheConfiguration<>()
                .setName(CACHE_NAME + '_' + SRV_RUN)
                .setCacheMode(CacheMode.PARTITIONED),
            new CacheConfiguration<>()
                .setName(CACHE_NAME + '_' + CLNT_RUN)
                .setCacheMode(CacheMode.PARTITIONED)
        };
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        IgniteEx srv = startGridAllowAll(SRV);

        startGridAllowAll(SRV_INITIATOR);

        startClientAllowAll(CLNT_INITIATOR);

        startGridAllowAll(SRV_RUN);

        startClientAllowAll(CLNT_RUN);

        startGridAllowAll(SRV_CHECK);

        srv.cluster().active(true);

        for(String cacheName : srv.cacheNames())
            srv.cache(cacheName).put(prmKey(grid(SRV_CHECK), cacheName), 1);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void setupVerifier(Verifier verifier) {
        verifier
            .expect(SRV_RUN, 1)
            .expect(CLNT_RUN, 1)
            .expect(SRV_CHECK, 2);
    }

    /**
     * Opens query cursor.
     *
     * @param q {@link Query}.
     */
    protected void openQueryCursor(Query<Cache.Entry<Integer, Integer>> q) {
        Ignite ignite = localIgnite();

        String cacheName = CACHE_NAME + '_' + ignite.name();

        IgniteCache<Integer, Integer> cache = ignite.cache(cacheName);

        try (QueryCursor<Cache.Entry<Integer, Integer>> cur = cache.query(q)) {
            cache.put(prmKey(grid(SRV_CHECK), cacheName), 100);
        }
    }
}
