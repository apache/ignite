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

package org.apache.ignite.yardstick.sql;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 * Ignite benchmark that performs query operations with joins.
 */
public class IgniteInlineIndexBenchmark extends IgniteAbstractBenchmark {
    /** Cache name for benchmark. */
    private String cacheName;

    /** */
    private Class<?> keyCls;

    /** How many entries should be preloaded and within which range. */
    private int range;

    /** Whether key should be type of Java object or simple (e.g. integer or long). */
    private boolean isJavaObj;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        range = args.range();
        isJavaObj = args.getBooleanParameter("javaObject", false);

        if (isJavaObj) {
            cacheName = "CACHE_POJO";
            keyCls = TestKey.class;
        }
        else {
            cacheName = "CACHE_LONG";
            keyCls = Integer.class;
        }

        printParameters();

        IgniteSemaphore sem = ignite().semaphore("setup", 1, true, true);

        try {
            if (sem.tryAcquire()) {
                println(cfg, "Create tables...");

                init();
            }
            else {
                // Acquire (wait setup by other client) and immediately release/
                println(cfg, "Waits for setup...");

                sem.acquire();
            }
        }
        finally {
            sem.release();
        }
    }

    /**
     *
     */
    private void init() {
        ignite().createCache(
            new CacheConfiguration<Object, Integer>(cacheName)
                .setIndexedTypes(keyCls, Integer.class)
        );

        println(cfg, "Populate cache: " + cacheName + ", range: " + range);

        try (IgniteDataStreamer<Object, Integer> stream = ignite().dataStreamer(cacheName)) {
            stream.allowOverwrite(false);

            for (long k = 0; k < range; ++k)
                stream.addData(nextKey(), ThreadLocalRandom.current().nextInt());
        }

        println(cfg, "Cache populated: " + cacheName);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        ignite().<Object, Integer>cache(cacheName).put(nextKey(), ThreadLocalRandom.current().nextInt());

        return true;
    }

    /** Creates next random key. */
    private Object nextKey() {
        return isJavaObj
            ? new TestKey(ThreadLocalRandom.current().nextInt(range))
            : ThreadLocalRandom.current().nextInt(range);
    }

    /** */
    private void printParameters() {
        println("Benchmark parameter:");
        println("    range: " + range);
        println("    is JavaObject: " + isJavaObj);
    }

    /** Pojo that used as key value for object's inlining benchmark. */
    static class TestKey {
        /** */
        private final int id;

        /** */
        public TestKey(int id) {
            this.id = id;
        }
    }
}
