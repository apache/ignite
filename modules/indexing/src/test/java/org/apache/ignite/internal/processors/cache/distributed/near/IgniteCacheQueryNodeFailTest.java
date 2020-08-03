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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test added to check for https://issues.apache.org/jira/browse/IGNITE-2542.
 */
public class IgniteCacheQueryNodeFailTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);
        ccfg.setBackups(0);
        ccfg.setIndexedTypes(Integer.class, Integer.class);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);

        startClientGrid(1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeFailedSimpleQuery()throws Exception {
        checkNodeFailed("select _key from Integer");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodeFailedReduceQuery()throws Exception {
        checkNodeFailed("select avg(_key) from Integer");
    }

    /**
     * @param qry Query.
     * @throws Exception If failed.
     */
    private void checkNodeFailed(final String qry) throws Exception {
        Ignite failSrv = startGrid(2);

        awaitPartitionMapExchange();

        assertFalse(failSrv.configuration().isClientMode());

        Ignite client = grid(1);

        final IgniteCache<Integer, Integer> cache = client.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100_000; i++)
            cache.put(i, i);

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                SqlFieldsQuery fieldsQry = new SqlFieldsQuery(qry);

                while (!stop.get()) {
                    try {
                        cache.query(fieldsQry).getAll();
                    }
                    catch (CacheException e) {
                        Throwable cause = e.getCause();

                        assertFalse("Unexpected cause: " + cause,
                            cause instanceof NullPointerException);
                    }
                }

                return null;
            }
        }, 20, "qry-thread");

        try {
            failSrv.close();

            U.sleep(100);
        }
        finally {
            stop.set(true);
        }

        fut.get();
    }
}
