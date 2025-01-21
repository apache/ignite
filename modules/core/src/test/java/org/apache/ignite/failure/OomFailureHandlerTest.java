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

package org.apache.ignite.failure;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_PHY_RAM;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Out of memory error failure handler test.
 */
public class OomFailureHandlerTest extends AbstractFailureHandlerTest {
    /** Listener log messages. */
    private static final ListeningTestLogger srvTestLog = new ListeningTestLogger(log);

    /** Oom failure logger message. */
    private static final String OOM_ON_RAM_FAILURE_LOG_MSG = "The total amount of RAM configured for nodes running on " +
        "the local host exceeds the recommended maximum value. This may lead to significant slowdown due to swapping, " +
        "or even JVM/Ignite crash with OutOfMemoryError (please decrease JVM heap size, data region size or checkpoint " +
        "buffer size) ";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(0)
        );

        cfg.setIncludeEventTypes(EventType.EVTS_ALL);

        cfg.setGridLogger(srvTestLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Test OOME in IgniteCompute.
     */
    @Test
    public void testComputeOomError() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        computeWithOomError(ignite0, ignite1);

        assertFailureState(ignite0, ignite1);
    }

    /**
     * Test OOME in EntryProcessor.
     */
    @Test
    public void testEntryProcessorOomError() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        IgniteCache<Integer, Integer> cache0 = ignite0.getOrCreateCache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, Integer> cache1 = ignite1.getOrCreateCache(DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        Integer key = primaryKey(cache1);

        cache1.put(key, key);

        try {
            IgniteFuture fut = cache0.invokeAsync(key, new EntryProcessor<Integer, Integer, Object>() {
                @Override public Object process(MutableEntry<Integer, Integer> entry,
                    Object... arguments) throws EntryProcessorException {
                    throw new OutOfMemoryError();
                }
            });

            fut.get();
        }
        catch (Throwable ignore) {
            // Expected.
        }

        assertFailureState(ignite0, ignite1);
    }

    /**
     * Test OOME in service method invocation.
     */
    @Test
    public void testServiceInvokeOomError() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        IgniteCache<Integer, Integer> cache1 = ignite1.getOrCreateCache(DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        Integer key = primaryKey(cache1);

        ignite0.services().deployKeyAffinitySingleton("fail-invoke-service", new FailServiceImpl(false),
            DEFAULT_CACHE_NAME, key);

        FailService svc = ignite0.services().serviceProxy("fail-invoke-service", FailService.class, false);

        try {
            svc.fail();
        }
        catch (Throwable ignore) {
            // Expected.
        }

        assertFailureState(ignite0, ignite1);
    }

    /**
     * Test OOME in service execute.
     */
    @Test
    public void testServiceExecuteOomError() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        IgniteCache<Integer, Integer> cache1 = ignite1.getOrCreateCache(DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        Integer key = primaryKey(cache1);

        ignite0.services().deployKeyAffinitySingleton("fail-execute-service", new FailServiceImpl(true),
            DEFAULT_CACHE_NAME, key);

        assertFailureState(ignite0, ignite1);
    }

    /**
     * Test OOME in event listener.
     */
    @Test
    public void testEventListenerOomError() throws Exception {
        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        IgniteCache<Integer, Integer> cache0 = ignite0.getOrCreateCache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, Integer> cache1 = ignite1.getOrCreateCache(DEFAULT_CACHE_NAME);

        awaitPartitionMapExchange();

        ignite1.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                throw new OutOfMemoryError();
            }
        }, EventType.EVT_CACHE_OBJECT_PUT);

        Integer key = primaryKey(cache1);

        try {
            cache0.put(key, key);
        }
        catch (Throwable ignore) {
            // Expected.
        }

        assertFailureState(ignite0, ignite1);
    }

    /**
     * Test OOME with maxed RAM usage in configuration.
     */
    @Test
    public void testConfigurationOomError() throws Exception {
        LogListener logLsnr0 = LogListener.matches(OOM_ON_RAM_FAILURE_LOG_MSG)
            .times(2)
            .build();

        srvTestLog.registerListener(logLsnr0);

        IgniteEx ignite0 = startGrid(0);

        long ram = ignite0.localNode().attribute(ATTR_PHY_RAM);

        IgniteEx ignite1 = startGridWithMaxedRamUsage(1, ram);

        computeWithOomError(ignite0, ignite1);

        assertFailureState(ignite0, ignite1);

        assertTrue(waitForCondition(logLsnr0::check, getTestTimeout()));
    }

    /**
     * @param idx int - Ignite instance index.
     * @param ram long - available RAM.
     * @return {@link IgniteEx} started with maxed RAM usage in configuration.
     */
    private IgniteEx startGridWithMaxedRamUsage(int idx, long ram) throws Exception {
        String igniteInstanceName = getTestIgniteInstanceName(idx);

        IgniteConfiguration cfg = getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setMaxSize((long)(0.9 * ram))));

        return (IgniteEx)startGrid(igniteInstanceName, optimize(cfg), null);
    }

    /**
     * @param ignite0 instance that starts compute.
     * @param ignite1 instance that handles compute and throws {@link OutOfMemoryError}.
     */
    private void computeWithOomError(IgniteEx ignite0, IgniteEx ignite1) {
        try {
            IgniteFuture<Boolean> res = ignite0.compute(ignite0.cluster().forNodeId(ignite1.cluster().localNode().id()))
                .callAsync(new IgniteCallable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        throw new OutOfMemoryError();
                    }
                });

            res.get();
        }
        catch (Throwable ignore) {
            // Expected.
        }
    }

    /**
     * @param igniteWork Working ignite instance.
     * @param igniteFail Failed ignite instance.
     */
    private static void assertFailureState(Ignite igniteWork, Ignite igniteFail) throws IgniteInterruptedCheckedException {
        assertTrue(waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return dummyFailureHandler(igniteFail).failure();
            }
        }, 5000L));

        assertFalse(dummyFailureHandler(igniteWork).failure());
    }

    /**
     *
     */
    private interface FailService extends Service {
        /**
         * Fail.
         */
        void fail();
    }

    /**
     *
     */
    private static class FailServiceImpl implements FailService {
        /** Fail on execute. */
        private final boolean failOnExec;

        /**
         * @param failOnExec Fail on execute.
         */
        private FailServiceImpl(boolean failOnExec) {
            this.failOnExec = failOnExec;
        }

        /** {@inheritDoc} */
        @Override public void fail() {
            throw new OutOfMemoryError();
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) throws Exception {
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            if (failOnExec)
                throw new OutOfMemoryError();
        }
    }
}
