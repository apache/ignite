/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.integration;

import java.io.Serializable;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.junit.Test;

/**
 *
 */
public abstract class IgniteCacheStoreNodeRestartAbstractTest extends IgniteCacheAbstractTest {
    /** */
    protected static final String CACHE_NAME1 = "cache1";

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheStore store = getStore(); // Use the same store instance for both caches.

        assert cfg.getCacheConfiguration().length == 1;

        CacheConfiguration ccfg0 = cfg.getCacheConfiguration()[0];

        ccfg0.setReadThrough(true);
        ccfg0.setWriteThrough(true);

        ccfg0.setCacheStoreFactory(singletonFactory(store));

        CacheConfiguration ccfg1 = cacheConfiguration(igniteInstanceName);

        ccfg1.setReadThrough(true);
        ccfg1.setWriteThrough(true);

        ccfg1.setName(CACHE_NAME1);

        ccfg1.setCacheStoreFactory(singletonFactory(store));

        cfg.setCacheConfiguration(ccfg0, ccfg1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /**
     * @returns Store.
     */
    protected abstract CacheStore getStore();

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMarshaller() throws Exception {
        grid(0).cache(CACHE_NAME1).put("key1", new UserObject("key1"));

        stopGrid(0);

        startGrid(1);

        //Checking that marshaller works correct after all nodes was stopped.
        UserObject obj = grid(1).<Object, UserObject>cache(CACHE_NAME1).get("key1");

        assert obj.field.equals("key1");
    }

    /**
     * Custom object.
     */
    private static class UserObject implements Serializable{
        /** */
        private String field;

        /**
         *
         */
        public UserObject(String field) {
            this.field = field;
        }

        /**
         *
         */
        public UserObject() {
        }

        /**
         *
         */
        public String getField() {
            return field;
        }
    }
}
