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

package org.apache.ignite.internal.processors.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.PartitionLossPolicy.READ_WRITE_SAFE;

/**
 *
 */
public class BinaryMetadataRegistrationInsideEntryProcessorTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "test-cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder()
            .setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));

        return new IgniteConfiguration()
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder))
            .setPeerClassLoadingEnabled(true);
    }

    /** Stop all grids after each test. */
    @After
    public void stopAllGridsAfterTest() {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed;
     */
    @Test
    public void test() throws Exception {
        Ignite ignite = startGrids(2);

        IgniteCache<Integer, Map<Integer, CustomObj>> cache = ignite.createCache(CACHE_NAME);

        try {
            for (int i = 0; i < 10_000; i++)
                cache.invoke(i, new CustomProcessor());
        }
        catch (Exception e) {
            Map<Integer, CustomObj> val = cache.get(1);

            if ((val != null) && (val.get(1).anEnum == CustomEnum.ONE) && val.get(1).obj.data.equals("test"))
                System.out.println("Data was saved.");
            else
                System.out.println("Data wasn't saved.");

            throw e;
        }
    }

    /**
     * Continuously execute multiple EntryProcessors with having continuous queries in parallel.
     * This used to lead to several deadlocks.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousQueryAndBinaryObjectBuilder() throws Exception {
        startGrids(3).cluster().active(true);

        grid(0).createCache(new CacheConfiguration<>()
            .setName(CACHE_NAME)
            .setAtomicityMode(ATOMIC)
            .setBackups(2)
            .setCacheMode(PARTITIONED)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setPartitionLossPolicy(READ_WRITE_SAFE)
        );

        IgniteEx client1 = startClientGrid(getConfiguration().setIgniteInstanceName("client1"));
        IgniteEx client2 = startClientGrid(getConfiguration().setIgniteInstanceName("client2"));

        AtomicBoolean stop = new AtomicBoolean();
        AtomicInteger keyCntr = new AtomicInteger();
        AtomicInteger binaryTypeCntr = new AtomicInteger();

        /** */
        class MyEntryProcessor implements CacheEntryProcessor<Object, Object, Object> {
            /** Cached int value retrieved from {@code binaryTypeCntr} variable. */
            private int i;

            /** */
            public MyEntryProcessor(int i) {
                this.i = i;
            }

            /** */
            @IgniteInstanceResource
            Ignite ignite;

            /** {@inheritDoc} */
            @Override public Object process(MutableEntry<Object, Object> entry, Object... arguments)
                throws EntryProcessorException {
                BinaryObjectBuilder builder = ignite.binary().builder("my_type");

                builder.setField("new_field" + i, i);

                entry.setValue(builder.build());

                return null;
            }
        }

        IgniteInternalFuture fut1 = GridTestUtils.runMultiThreadedAsync(() -> {
            IgniteCache<Object, Object> cache = client1.cache(CACHE_NAME).withKeepBinary();

            while (!stop.get()) {
                Integer key = keyCntr.getAndIncrement();

                cache.put(key, key);

                cache.invoke(key, new MyEntryProcessor(binaryTypeCntr.get()));

                binaryTypeCntr.incrementAndGet();
            }
        }, 8, "writer-thread");

        IgniteInternalFuture fut2 = GridTestUtils.runAsync(() -> {
            IgniteCache<Object, Object> cache = client2.cache(CACHE_NAME).withKeepBinary();

            while (!stop.get()) {
                ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

                qry.setInitialQuery(new ScanQuery<>((key, val) -> true));

                qry.setLocalListener(evts -> {});

                //noinspection EmptyTryBlock
                try (QueryCursor<Cache.Entry<Object, Object>> cursor = cache.query(qry)) {
                    // No-op.
                }
            }
        });

        doSleep(10_000);

        stop.set(true);

        fut1.get(10, TimeUnit.SECONDS);
        fut2.get(10, TimeUnit.SECONDS);
    }

    /**
     *
     */
    private static class CustomProcessor implements EntryProcessor<Integer,
        Map<Integer, CustomObj>, Object> {
        /** {@inheritDoc} */
        @Override public Object process(
            MutableEntry<Integer, Map<Integer, CustomObj>> entry,
            Object... objects) throws EntryProcessorException {
            Map<Integer, CustomObj> map = new HashMap<>();

            map.put(1, new CustomObj(new CustomInnerObject("test"), CustomEnum.ONE));

            entry.setValue(map);

            return null;
        }
    }

    /**
     *
     */
    private static class CustomObj {
        /** Object. */
        private final CustomInnerObject obj;

        /** Enum. */
        private final CustomEnum anEnum;

        /**
         * @param obj Object.
         * @param anEnum Enum.
         */
        CustomObj(
            CustomInnerObject obj,
            CustomEnum anEnum) {
            this.obj = obj;
            this.anEnum = anEnum;
        }
    }

    /**
     *
     */
    private enum CustomEnum {
        /** */ONE(1),
        /** */TWO(2),
        /** */THREE(3);

        /** Value. */
        private final Object val;

        /**
         * @param val Value.
         */
        CustomEnum(Object val) {
            this.val = val;
        }
    }

    /**
     *
     */
    private static class CustomInnerObject {
        /** */
        private final String data;

        /**
         * @param data Data.
         */
        CustomInnerObject(String data) {
            this.data = data;
        }
    }
}
