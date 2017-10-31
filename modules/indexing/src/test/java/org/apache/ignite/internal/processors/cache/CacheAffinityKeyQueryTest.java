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

package org.apache.ignite.internal.processors.cache;

import java.util.*;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests affinity key and {@link QueryEntity} availability on all nodes.
 */
public class CacheAffinityKeyQueryTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Server node name. */
    private static final String SERVER = "server";

    /** Client node name. */
    private static final String CLIENT = "client";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.equals(CLIENT))
            cfg.setClientMode(true);
        else {
            cfg.setCacheConfiguration(
                new CacheConfiguration(CACHE_NAME).setQueryEntities(
                    Collections.singleton(new QueryEntity(Key.class.getName(), String.class.getName()))));
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    public void testQueryDescriptorPicksAffinityKey() throws Exception {
        Ignite srv = startGrid(SERVER);

        Collection<GridQueryTypeDescriptor> types = ((IgniteEx)srv).context().query().types(CACHE_NAME);

        assertTrue(types.size() > 0);

        assertEquals("AFFKEY", types.iterator().next().affinityKey());
    }

    /** */
    public void testClientFindsValueByAffinityKey() throws Exception {
        final Key KEY = new Key("1", 1);
        final String VAL = "1";

        Ignite srv = startGrid(SERVER);

        IgniteCache<Key, String> srvCache = srv.cache(CACHE_NAME);

        srvCache.put(KEY, VAL);

        assertEquals(VAL, srvCache.get(KEY));

        Ignite client = startGrid(CLIENT);

        IgniteCache<Key, String> clientCache = client.cache(CACHE_NAME);

        assertEquals(VAL, clientCache.get(KEY));
    }

    /** */
    private static class Key {
        /** Data */
        private String data;

        /** Affinity key */
        @AffinityKeyMapped
        private int affKey;

        /**
         * @param data Data.
         * @param affKey Aff key.
         */
        public Key(String data, int affKey) {
            this.data = data;
            this.affKey = affKey;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key key = (Key)o;

            return affKey == key.affKey && (data != null ? data.equals(key.data) : key.data == null);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = data != null ? data.hashCode() : 0;

            res = 31 * res + affKey;

            return res;
        }
    }
}
