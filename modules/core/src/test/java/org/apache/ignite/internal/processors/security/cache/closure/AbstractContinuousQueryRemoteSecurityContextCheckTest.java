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

import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
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
    protected static CacheEntryEventSerializableFilter<Integer, Integer> createRemoteFilter() {
        return new CacheEntryEventSerializableFilter<Integer, Integer>() {
            /** Should be registred one time only. */
            private final AtomicBoolean registred = new AtomicBoolean(false);

            @Override public boolean evaluate(
                CacheEntryEvent<? extends Integer, ? extends Integer> evt) throws CacheEntryListenerException {
                if (!registred.getAndSet(true))
                    VERIFIER.register();

                return true;
            }
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

        srv.cache(CACHE_NAME).put(prmKey(grid(SRV_CHECK)), 1);

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
        IgniteCache<Integer, Integer> cache = localIgnite().cache(CACHE_NAME);

        try (QueryCursor<Cache.Entry<Integer, Integer>> cur = cache.query(q)) {
            cache.put(prmKey(grid(SRV_CHECK)), 100);
        }
    }
}
