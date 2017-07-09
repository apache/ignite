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
package org.apache.ignite.internal.processors.datastreamer;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.stream.StreamTransformer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Switching class loaders during object deserialization test.
 *
 * @see <a href="http://google.com">https://issues.apache.org/jira/browse/IGNITE-3935</a>
 */
public class DataStreamerClassLoaderTest extends GridCommonAbstractTest {
    /** Cache name. */
    public static final String CACHE_NAME = "cacheName";

    /** {@inheritDoc} */
    protected void beforeTest() throws Exception {
        super.beforeTest();
        startGrid(1);
        System.out.println("Server mode started");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
        System.out.println("Stopping client mode");
        Ignition.stop(false);
        Ignition.setClientMode(false);
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        MemoryConfiguration memCfg = new MemoryConfiguration();

        MemoryPolicyConfiguration plcCfg = new MemoryPolicyConfiguration();

        plcCfg.setInitialSize(20 * 1000 * 1000);
        plcCfg.setMaxSize(200 * 1000 * 1000);
        plcCfg.setName("myPolicy");

        memCfg.setMemoryPolicies(plcCfg);
        cfg.setMemoryConfiguration(memCfg);
        cfg.setCacheConfiguration(cacheConfiguration());
        memCfg.setDefaultMemoryPolicyName("myPolicy");
        return cfg;
    }

    /**
     * Gets cache configuration.
     *
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration() {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setName(CACHE_NAME);

        return cacheCfg;
    }

    /**
     * Check correct case
     *
     * @throws Exception If failed.
     */
    public void testCheckClassLoaders1() throws Exception {
        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println("Client mode started");
            IgniteCache<String, Long> stmCache = ignite.getOrCreateCache("mycache");
            try (IgniteDataStreamer<String, Long> stmr = ignite.dataStreamer(stmCache.getName())) {
                stmr.allowOverwrite(true);
                stmr.receiver(new StreamingExampleTransformer());
                stmr.addData("word", 1L);
                System.out.println("Finished");
            }
        }
    }

    /**
     * Check incorrect case
     */
    public void testCheckClassLoaders2() {
        boolean exceptionThrown = false;
        try {
            Ignition.setClientMode(true);
            try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
                System.out.println("Client mode started");
                IgniteCache<String, Long> stmCache = ignite.getOrCreateCache("mycache");
                try (IgniteDataStreamer<String, Long> stmr = ignite.dataStreamer(stmCache.getName())) {
                    stmr.allowOverwrite(true);
                    stmr.receiver(StreamTransformer.from(new StreamingExampleCacheEntryProcessor()));
                    stmr.addData("word", 1L);
                    System.out.println("Finished");
                }
            }
        }
        catch (Exception e) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    public static class StreamingExampleTransformer extends StreamTransformer<String, Long> {
        @Override
        public Object process(MutableEntry<String, Long> e, Object... arg) throws EntryProcessorException {
            System.out.println("Executed!");
            Long val = e.getValue();
            e.setValue(val == null ? 1L : val + 1);
            return null;
        }
    }

    public static class StreamingExampleCacheEntryProcessor implements CacheEntryProcessor<String, Long, Object> {
        @Override
        public Object process(MutableEntry<String, Long> e, Object... arg) throws EntryProcessorException {
            System.out.println("Executed!");
            Long val = e.getValue();
            e.setValue(val == null ? 1L : val + 1);
            return null;
        }
    }

}
