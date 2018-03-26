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

package org.apache.ignite.internal.processors.query;

import java.util.UUID;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Test for IGNITE-6807
 */
public class ClientLocalQueryTest extends GridCommonAbstractTest {
    /** Client node. Shared across test methods. */
    private static Ignite client;

    /** Name of created cache */
    private static final String CACHE_NAME = "TestCache";

    /** Starts node with specified config. */
    Ignite startGridWithCfg(String name, IgniteConfiguration cfg) throws Exception {
        return startGrid(name, optimize(cfg), null);
    }

    /** Sets up grids */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid("server");

        IgniteConfiguration clCfg = getConfiguration();
        clCfg.setClientMode(true);

        client = startGridWithCfg("client", clCfg);
    }

    /** Creates cache and test table for the test */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        CacheConfiguration<Object, UUID> ccfg = new CacheConfiguration<>(CACHE_NAME);
        ccfg.setIndexedTypes(Object.class, UUID.class);

        client.createCache(ccfg);

        log.info("Created cache with cfg: " + ccfg);
    }

    /** Destroy the cache. */
    @Override protected void afterTest() throws Exception {
        client.destroyCache(CACHE_NAME);

        super.afterTest();
    }

    /** Stops all grids. */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * If we are executing local query on client node, we will get reasonable exception message.
     *
     * @throws Exception on error.
     */
    public void testLocalQueryIsUnsupportedOnClient() throws Exception {
        IgniteCache<Object, UUID> cache = client.cache(CACHE_NAME);

        //SqlFieldsQuery qry = new SqlFieldsQuery("SELECT count(id) FROM public.test_table;").setLocal(true);
        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT count(_key) FROM UUID;").setLocal(true);

        assertExceptionThrown(() -> cache.query(qry).getAll(),
            ".*Local queries are NOT supported on client nodes.*",
            CacheException.class);
    }

    public void testOtherQuery() throws Exception {
        SqlQuery<Object, UUID> qry = new SqlQuery<>(UUID.class, "1 = 1");

        Runnable throwing = () -> client.cache(CACHE_NAME).query(qry.setLocal(true)).getAll();

        assertExceptionThrown(throwing, ".*Local queries are NOT supported on client nodes.*", CacheException.class);
    }

    public void testTextQuery() throws Exception {
        TextQuery<Object, UUID> qry = new TextQuery<>(UUID.class, "doesn't matter");

        Runnable throwing = () -> client.cache(CACHE_NAME).query(qry.setLocal(true)).getAll();

        assertExceptionThrown(throwing, ".*Local queries are NOT supported on client nodes.*", CacheException.class);
    }

    private void assertExceptionThrown(Runnable action, String msgPattern,
        @Nullable Class<? extends Throwable> expType) throws Exception {
        if (expType == null)
            expType = Throwable.class;

        boolean throwMissed = false;

        try {
            action.run();

            throwMissed = true;
        }
        catch (Throwable th) {
            String msg = th.getMessage();

            if (!expType.isInstance(th))
                throw new AssertionError("Throwable of unexpected type have been thrown. [actual="
                    + th.getClass().getCanonicalName() + ", expected=" + expType.getSimpleName()
                    + "]. See full exception in the cause.", th);

            if (!msg.matches(msgPattern))
                throw new AssertionError("Message of thrown exception doesn't match the pattern: [pattern="
                    + msgPattern + ", message=" + msg + "]. See full exception in the cause.", th);
        }

        if (throwMissed)
            throw new AssertionError("Expected Throwable of type " + expType.getCanonicalName() +
                " have not been thrown.");

    }
}
