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

package org.apache.ignite.rs;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;

import javax.cache.Cache;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IgniteReactiveStreamTest extends GridCommonAbstractTest {

    private static final int CACHE_ENTRY_COUNT = 100;

    /** Constructor. */
    public IgniteReactiveStreamTest() {
        super(true);
    }

    @Before @SuppressWarnings("unchecked")
    public void beforeTest() throws Exception {
        grid().<String, String>getOrCreateCache(defaultCacheConfiguration());

    }

    @After
    public void afterTest() throws Exception {
        grid().cache(null).clear();
    }

    public void testRsCQ() throws InterruptedException {
        IgniteCache<Integer, String> cache = grid().cache(null);
        cache.clear();
        cache.put(100, "1 existing");
        ReactiveContinuousQuery<Integer, String> continuousQuery = new ReactiveContinuousQuery<>(grid().cache(null), new IgniteBiPredicate<Integer, String>() {
            @Override public boolean apply(Integer key, String val) {
                return true;
            }
        });

        //simulate data push to cache
        ExecutorService service = Executors.newSingleThreadExecutor();
        service.submit(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 100; i++){
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    cache.put(i, Integer.toString(i));
                }
            }
        });

        continuousQuery.subscribe(new IgniteSubscriber<Cache.Entry<Integer, String>>());

        Thread.sleep(50000);
        //add un-subscribe
    }

}
