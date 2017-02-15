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

package org.apache.ignite.internal.processors.database;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.internal.IgniteEx;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class IgniteDbMemoryLeakLargePagesTest extends IgniteDbMemoryLeakAbstractTest {

    @Override
    protected int duration() {
        return 300;
    }

    @Override
    protected int gridCount() {
        return 1;
    }

    @Override
    protected void configure(IgniteConfiguration cfg) {
        cfg.setMetricsLogFrequency(5000);
    }

    @Override
    protected void configure(MemoryConfiguration mCfg) {
        mCfg.setPageCacheSize(100 * 1024 * 1024);
    }

    @Override
    protected boolean indexingEnabled() {
        return false;
    }

    @Override
    protected boolean isLargePage() {
        return true;
    }

    protected void operation(IgniteEx ig){
        IgniteCache<Object, Object> cache = ig.cache("non-primitive");
        Random rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 1000; i++) {
            DbKey key = new DbKey(rnd.nextInt(200_000));

            DbValue v0 = new DbValue(key.val, "test-value-" + rnd.nextInt(200), rnd.nextInt(500));

            switch (rnd.nextInt(3)) {
                case 0:
                    cache.getAndPut(key, v0);
                case 1:
                    cache.get(key);
                    break;
                case 2:
                    cache.getAndRemove(key);
            }
        }
    }

    @Override
    protected void check(IgniteEx ig) {
        long pages = ig.context().cache().context().database().pageMemory().loadedPages();

        assertTrue(pages < 4600);
    }
}
