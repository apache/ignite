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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.marshaller.portable.*;
import org.apache.ignite.portable.*;
import org.apache.ignite.testframework.junits.common.*;

import org.jsr166.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Test for portable objects stored in cache.
 */
public abstract class GridCachePortableObjectsAbstractMultiThreadedSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int THREAD_CNT = 64;

    /** */
    private static final AtomicInteger idxGen = new AtomicInteger();

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
     * @return Distribution mode.
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
    @SuppressWarnings("BusyWait") public void testGetPut() throws Exception {
        final AtomicBoolean flag = new AtomicBoolean();

        final LongAdder8 cnt = new LongAdder8();

        IgniteInternalFuture<?> f = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    int threadId = idxGen.getAndIncrement() % 2;

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!flag.get()) {
                        IgniteCache<Object, Object> c = jcache(rnd.nextInt(gridCount()));

                        switch (threadId) {
                            case 0:
                                // Put/get/remove portable -> portable.

                                c.put(new TestObject(rnd.nextInt(10000)), new TestObject(rnd.nextInt(10000)));

                                IgniteCache<Object, Object> p2 = ((IgniteCacheProxy<Object, Object>)c).keepPortable();

                                PortableObject v = (PortableObject)p2.get(new TestObject(rnd.nextInt(10000)));

                                if (v != null)
                                    v.deserialize();

                                c.remove(new TestObject(rnd.nextInt(10000)));

                                break;

                            case 1:
                                // Put/get int -> portable.
                                c.put(rnd.nextInt(10000), new TestObject(rnd.nextInt(10000)));

                                IgniteCache<Integer, PortableObject> p4 = ((IgniteCacheProxy<Object, Object>)c).keepPortable();

                                PortableObject v1 = p4.get(rnd.nextInt(10000));

                                if (v1 != null)
                                    v1.deserialize();

                                p4.remove(rnd.nextInt(10000));

                                break;

                            default:
                                assert false;
                        }

                        cnt.add(3);
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
