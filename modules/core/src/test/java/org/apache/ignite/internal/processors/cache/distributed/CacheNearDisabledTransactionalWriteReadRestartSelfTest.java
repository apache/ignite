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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Transactional write read consistency test.
 */
public class CacheNearDisabledTransactionalWriteReadRestartSelfTest extends CacheAbstractRestartSelfTest {
    /** */
    public static final int RANGE = 100;

    /** */
    private static final int KEYS_CNT = 5;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected void checkCache(IgniteEx ignite, IgniteCache cache) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void updateCache(IgniteEx ignite, final IgniteCache cache) throws Exception {
        final int k = ThreadLocalRandom.current().nextInt(RANGE);

        final String[] keys = new String[KEYS_CNT];

        for (int i = 0; i < keys.length; i++)
            keys[i] = "key-" + k + "-" + i;

        doInTransaction(ignite, new Callable<Void>() {
            @Override public Void call() throws Exception {
                Map<String, Long> map = new HashMap<>();

                for (String key : keys) {
                    Long val = (Long)cache.get(key);

                    map.put(key, val);
                }

                Set<Long> values = new HashSet<>(map.values());

                if (values.size() != 1) {
                    // Print all usefull information and finish.
                    U.error(log, "Got different values for keys [map=" + map + "]");

                    log.info("Cache content:");

                    for (int k = 0; k < RANGE; k++) {
                        for (int i = 0; i < KEYS_CNT; i++) {
                            String key = "key-" + k + "-" + i;

                            Long val = (Long)cache.get(key);

                            if (val != null)
                                log.info("Entry [key=" + key + ", val=" + val + "]");
                        }
                    }

                    throw new IllegalStateException("Found different values for keys (see above information) [map="
                        + map + ']');
                }

                final Long oldVal = map.get(keys[0]);

                final Long newVal = oldVal == null ? 0 : oldVal + 1;

                for (String key : keys)
                    cache.put(key, newVal);

                return null;
            }
        });
    }
}
