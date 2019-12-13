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
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.processors.security.AbstractCacheOperationRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;

import static org.apache.ignite.Ignition.localIgnite;

/**
 * The base class for {@code ContinuousQuery} remote {@code SecurityContext} tests.
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

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridAllowAll(SRV);

        startGridAllowAll(SRV_INITIATOR);

        startClientAllowAll(CLNT_INITIATOR);

        startGridAllowAll(SRV_RUN);

        startClientAllowAll(CLNT_RUN);

        startGridAllowAll(SRV_CHECK);

        G.allGrids().get(0).cluster().active(true);

        grid(SRV).cache(CACHE_NAME).put(prmKey(grid(SRV_CHECK)), 1);

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
     * Opens {@code QueryCursor}.
     */
    protected void openQueryCursor(Query<Cache.Entry<Integer, Integer>> q) {
        try (QueryCursor<Cache.Entry<Integer, Integer>> cur = localIgnite().cache(CACHE_NAME).query(q)) {
            grid(SRV).cache(CACHE_NAME).put(prmKey(grid(SRV_CHECK)), 100);

            cur.getAll();
        }
    }

    /** Test remote filter factory. */
    protected static class TestRemoteFilterFactory implements Factory<CacheEntryEventFilter<Integer, Integer>> {
        /** {@inheritDoc} */
        @Override public CacheEntryEventFilter<Integer, Integer> create() {
            return new TestCacheEntryEventFilter();
        }
    }

    /** Test remote filter. */
    protected static class TestCacheEntryEventFilter implements CacheEntryEventSerializableFilter<Integer, Integer> {
        /** Calling of filter should be registered one time only. */
        private final AtomicBoolean executed = new AtomicBoolean(false);

        /** {@inheritDoc} */
        @Override public boolean evaluate(
            CacheEntryEvent<? extends Integer, ? extends Integer> evt) throws CacheEntryListenerException {
            if (!executed.getAndSet(true))
                VERIFIER.register();

            return false;
        }
    }
}
