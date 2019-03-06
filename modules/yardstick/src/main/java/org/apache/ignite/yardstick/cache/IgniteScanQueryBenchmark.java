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

package org.apache.ignite.yardstick.cache;

import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.yardstickframework.BenchmarkConfiguration;

/**
 *
 */
public class IgniteScanQueryBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        loadCachesData();
    }

    /** {@inheritDoc} */
    @Override protected void loadCacheData(String cacheName) {
        loadSampleValues(cacheName, args.range());
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int key = nextRandom(args.range());

        ScanQuery<Integer, Object> qry = new ScanQuery<>();

        qry.setFilter(new KeyFilter(key));

        IgniteCache<Integer, Object> cache = cacheForOperation().withKeepBinary();

        List<IgniteCache.Entry<Integer, Object>> res = cache.query(qry).getAll();

        if (res.size() != 1)
            throw new Exception("Invalid result size: " + res.size());

        if (res.get(0).getKey() != key)
            throw new Exception("Invalid entry found [key=" + key + ", entryKey=" + res.get(0).getKey() + ']');

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("atomic");
    }

    /**
     *
     */
    static class KeyFilter implements IgniteBiPredicate<Integer, Object> {
        /** */
        private final Integer key;

        /**
         * @param key Key to find.
         */
        public KeyFilter(Integer key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Integer key, Object val) {
            return this.key.equals(key);
        }
    }
}
