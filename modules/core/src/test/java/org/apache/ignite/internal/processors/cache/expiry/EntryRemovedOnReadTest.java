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

package org.apache.ignite.internal.processors.cache.expiry;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.REPLICATED;

/** todo remove */
public class EntryRemovedOnReadTest extends GridCommonAbstractTest {
    /** */
    private final ListeningTestLogger log = new ListeningTestLogger(GridAbstractTest.log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(log)
            .setCacheConfiguration(new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(REPLICATED)
            );
    }

    /** */
    @Test
    public void test() throws Exception {
        LogListener lsnr = LogListener.matches("Failed to send TTL update request").build();

        log.registerListener(lsnr);

        startGridsMultiThreaded(4);

        Map<Integer, Integer> vals = new TreeMap<>();

        for (int i = 1; i < 10; i++)
            vals.put(i, i);

        jcache(0).putAll(vals);

        assertFalse(lsnr.check());

        long timeout = 20_000L;
        long stopTime = System.currentTimeMillis() + timeout;
        int iter = 0;

        IgniteCache<Object, Object> cache = jcache(1).withExpiryPolicy(new ExpiryPolicy() {
            @Override public Duration getExpiryForAccess() {
                return new Duration(TimeUnit.MILLISECONDS, timeout);
            }

            @Override public Duration getExpiryForCreation() { return null; }

            @Override public Duration getExpiryForUpdate() { return null; }
        });

        while (System.currentTimeMillis() < stopTime) {
            cache.getAll(vals.keySet());

            assertFalse("iter=" + ++iter, lsnr.check());
        }
    }
}
