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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class CacheKeepBinaryIterationStoreEnabledTest extends CacheKeepBinaryIterationTest {
    /** Cache store. */
    private static TestStore store = new TestStore();

    /** {@inheritDoc} */
    @Override protected CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        int backups,
        CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Object, Object> ccfg =
            super.cacheConfiguration(cacheMode, backups, atomicityMode);

        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        ccfg.setCacheStoreFactory(singletonFactory(store));
        ccfg.setReadThrough(true);
        ccfg.setWriteThrough(true);
        ccfg.setLoadPreviousValue(true);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8582")
    @Test
    @Override public void testMvccTxOnHeap() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-8582")
    @Test
    @Override public void testMvccTxOnHeapLocalEntries() throws Exception {
        // No-op
    }

    /**
     *
     */
    private static class TestStore extends CacheStoreAdapter<Object, Object> {
        /** Map. */
        private ConcurrentMap<Object, Object> map = new ConcurrentHashMap<>();

        /**
         * @param key Key.
         * @return Value.
         */
        Object value(Object key) {
            return map.get(key);
        }

        /** @return {@code True} if empty. */
        boolean isEmpty() {
            return map.isEmpty();
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) {
            return map.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(javax.cache.Cache.Entry<? extends Object, ? extends Object> e) {
            map.put(e.getKey(), e.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            map.remove(key);
        }
    }
}
