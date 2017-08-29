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

package org.apache.ignite.internal;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

/**
 *
 */
public class IgniteClientReconnectBinaryContexTest extends IgniteClientReconnectAbstractTest {
    /** {@inheritDoc} */
    @Override protected int serverCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected int clientCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryContexReconnectClusterRestart() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = grid(0);

        CacheConfiguration<TestClass1, TestClass1> personCache = new CacheConfiguration<TestClass1, TestClass1>()
            .setName(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.REPLICATED);

        srv.createCache(personCache);

        client.cache(DEFAULT_CACHE_NAME).put(new TestClass1("1"), new TestClass1("1"));
        client.cache(DEFAULT_CACHE_NAME).put(new TestClass1("2"), new TestClass1("2"));

        Collection<Ignite> ignites = reconnectServersRestart(log, client, Collections.singleton(srv), new Callable<Collection<Ignite>>() {
            @Override public Collection<Ignite> call() throws Exception {
                return Collections.singleton((Ignite)startGrid(0));
            }
        });

        ignites.iterator().next().createCache(personCache);

        client.cache(DEFAULT_CACHE_NAME).put(new TestClass1("2"), new TestClass1("2"));
        client.cache(DEFAULT_CACHE_NAME).put(new TestClass1("3"), new TestClass1("3"));

        int size = 0;

        for (Cache.Entry<TestClass1, TestClass1> entry : client.<TestClass1, TestClass1>cache(DEFAULT_CACHE_NAME))
            size++;

        assertTrue(size == 2);
    }

    /**
     *
     */
    static class TestClass1 {
        /** */
        final String val;

        /**
         * @param val Value.
         */
        TestClass1(String val) {
            this.val = val;
        }
    }
}
