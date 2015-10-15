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

package org.apache.ignite.internal.processors.cache.portable;

import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.portable.PortableException;
import org.apache.ignite.portable.PortableMarshalAware;
import org.apache.ignite.portable.PortableReader;
import org.apache.ignite.portable.PortableTypeConfiguration;
import org.apache.ignite.portable.PortableWriter;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.LongAdder8;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

/**
 * Test for portable objects stored in cache.
 */
public abstract class GridCachePortableObjectsAbstractDataStreamerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int THREAD_CNT = 64;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setCacheMode(cacheMode());
        cacheCfg.setAtomicityMode(atomicityMode());
        cacheCfg.setNearConfiguration(nearConfiguration());
        cacheCfg.setWriteSynchronizationMode(writeSynchronizationMode());

        cfg.setCacheConfiguration(cacheCfg);

        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(TestObject.class.getName())));

        cfg.setMarshaller(marsh);

        return cfg;
    }

    /**
     * @return Sync mode.
     */
    protected CacheWriteSynchronizationMode writeSynchronizationMode() {
        return PRIMARY_SYNC;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode cacheMode();

    /**
     * @return Atomicity mode.
     */
    protected abstract CacheAtomicityMode atomicityMode();

    /**
     * @return Near configuration.
     */
    protected abstract NearCacheConfiguration nearConfiguration();

    /**
     * @return Grid count.
     */
    protected int gridCount() {
        return 1;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("BusyWait")
    public void testGetPut() throws Exception {
        final AtomicBoolean flag = new AtomicBoolean();

        final LongAdder8 cnt = new LongAdder8();

        try (IgniteDataStreamer<Object, Object> ldr = grid(0).dataStreamer(null)) {
            IgniteInternalFuture<?> f = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        while (!flag.get()) {
                            ldr.addData(rnd.nextInt(10000), new TestObject(rnd.nextInt(10000)));

                            cnt.add(1);
                        }

                        return null;
                    }
                },
                THREAD_CNT
            );

            for (int i = 0; i < 30 && !f.isDone(); i++)
                Thread.sleep(1000);

            flag.set(true);

            f.get();
        }

        info("Operations in 30 sec: " + cnt.sum());
    }

    /**
     */
    private static class TestObject implements PortableMarshalAware, Serializable {
        /** */
        private int val;

        /**
         */
        private TestObject() {
            // No-op.
        }

        /**
         * @param val Value.
         */
        private TestObject(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof TestObject && ((TestObject)obj).val == val;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(PortableWriter writer) throws PortableException {
            writer.writeInt("val", val);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(PortableReader reader) throws PortableException {
            val = reader.readInt("val");
        }
    }
}