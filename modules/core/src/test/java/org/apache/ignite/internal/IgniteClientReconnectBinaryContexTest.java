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
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;

/**
 *
 */
public class IgniteClientReconnectBinaryContexTest extends IgniteClientReconnectAbstractTest {
    /** */
    public static final String DEFAULT_CACHE_NAME = "myCache";

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

        cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectCleaningUsersMetadata() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = grid(0);

        srv.destroyCache(DEFAULT_CACHE_NAME);

        CacheConfiguration<Integer, UserClass> cacheCfg = new CacheConfiguration<Integer, UserClass>()
            .setName(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.REPLICATED);

        IgniteCache<Integer, UserClass> cache = client.createCache(cacheCfg);

        Integer key = 1;
        UserClass val = new UserClass(1);

        cache.put(key, val); // For registering user types binary metadata

        reconnectServersRestart(log, client, Collections.singleton(srv), new Callable<Collection<Ignite>>() {
            /** {@inheritDoc} */
            @Override public Collection<Ignite> call() throws Exception {
                return Collections.singleton((Ignite)startGrid(0));
            }
        });

        cache = client.createCache(cacheCfg);

        cache.put(key, val);

        assertEquals(val, cache.get(key));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReconnectCleaningUsersMetadata2() throws Exception {
        Ignite client = grid(serverCount());

        assertTrue(client.cluster().localNode().isClient());

        Ignite srv = grid(0);

        srv.destroyCache(DEFAULT_CACHE_NAME);

        CacheConfiguration<Integer, UserClass> cacheCfg = new CacheConfiguration<Integer, UserClass>()
            .setName(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.REPLICATED);

        IgniteCache<Integer, UserClass> cache = client.createCache(cacheCfg);

        Integer key = 1;
        final UserClass val = new UserClass(1);

        EntryProcessor<Integer, UserClass, Object> ep = new DummyEntryProccessor(val);

        cache.invoke(key, ep); // For registering user types binary metadata

        reconnectServersRestart(log, client, Collections.singleton(srv), new Callable<Collection<Ignite>>() {
            /** {@inheritDoc} */
            @Override public Collection<Ignite> call() throws Exception {
                return Collections.singleton((Ignite)startGrid(0));
            }
        });

        cache = client.createCache(cacheCfg);

        cache.invoke(key, ep);

        assertEquals(val, cache.get(key));
    }

    /** */
    private static class UserClass {
        /** */
        private final int field;

        /**
         * @param field Value.
         */
        private UserClass(int field) {
            this.field = field;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            UserClass val = (UserClass)o;

            return field == val.field;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return field;
        }
    }

    /** */
    private static class DummyEntryProccessor implements EntryProcessor<Integer, UserClass, Object> {
        /** */
        private final UserClass val;

        /** Constructor. */
        public DummyEntryProccessor(UserClass val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public UserClass process(MutableEntry<Integer, UserClass> entry, Object... arguments)
            throws EntryProcessorException {
            entry.setValue(val);

            return null;
        }
    }
}
