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
package org.apache.ignite.cache;

import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.internal.mem.OutOfMemoryException;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * LargeEntryUpdateTest
 *
 * @author Alexandr Kuramshin <ein.nsk.ru@gmail.com>
 */
public class LargeEntryUpdateTest extends GridCommonAbstractTest {

    /**  */
    private static final int THREAD_COUNT = 10;

    /**  */
    private static final int PAGE_SIZE = 1 << 10; // 1 kB

    /**  */
    private static final int PAGE_CACHE_SIZE = 30 << 20; // 30 MB

    /**  */
    private static final String CACHE_PREFIX = "testCache";

    /**  */
    private static final int CACHE_COUNT = 10;

    /**  */
    private static final long WAIT_TIMEOUT = 5 * 60_000L; // 5 min

    /**  */
    private static final long TEST_TIMEOUT = 10 * 60_000L; // 10 min

    /**  */
    private final AtomicBoolean cacheUpdate = new AtomicBoolean();

    /**  */
    private OutOfMemoryException outOfMemoryEx;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);
        cfg.setPublicThreadPoolSize(THREAD_COUNT);

        MemoryConfiguration mem = new MemoryConfiguration();
        mem.setPageSize(PAGE_SIZE);
        mem.setPageCacheSize(PAGE_CACHE_SIZE);
        cfg.setMemoryConfiguration(mem);

        CacheConfiguration[] ccfgs = new CacheConfiguration[CACHE_COUNT];
        for (int i = 0; i < CACHE_COUNT; ++i) {
            CacheConfiguration ccfg = new CacheConfiguration();
            ccfg.setName(CACHE_PREFIX + i);
            ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
            ccfg.setCacheMode(CacheMode.PARTITIONED);
            ccfgs[i] = ccfg;
        }
        cfg.setCacheConfiguration(ccfgs);

        return cfg;
    }

    /**  */
    public void testEntryUpdate() throws Exception {
        try (Ignite ignite = startGrid()) {
            for (int i = 0; i < CACHE_COUNT; ++i) {
                IgniteCache<Long, byte[]> cache = ignite.cache(CACHE_PREFIX + i);
                cache.put(0L, new byte[PAGE_SIZE * 2]);
            }
            cacheUpdate.set(true);
            IgniteCompute compute = ignite.compute().withAsync();
            for (int i = 0; i < THREAD_COUNT; ++i)
                compute.run(new CacheUpdater());
            try {
                long deadline = System.currentTimeMillis() + WAIT_TIMEOUT;
                while (true)
                    synchronized (this) {
                        if (outOfMemoryEx != null)
                            throw outOfMemoryEx;
                        long timeout = deadline - System.currentTimeMillis();
                        if (timeout <= 0)
                            break;
                        wait(timeout);
                    }
            }
            finally {
                cacheUpdate.set(false);
            }
        }
    }

    /**  */
    private synchronized void setOutOfMemoryEx(OutOfMemoryException e) {
        outOfMemoryEx = e;
        notifyAll();
    }

    /**  */
    public static class EntryUpdater implements CacheEntryProcessor<Long, byte[], Void> {

        /**  */
        public static final EntryUpdater INSTANCE = new EntryUpdater();

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Long, byte[]> entry, Object... arguments)
            throws EntryProcessorException {

            entry.setValue(new byte[PAGE_SIZE]);
            return null;
        }
    }

    /**  */
    public class CacheUpdater implements IgniteRunnable {

        /**  */
        @IgniteInstanceResource
        public transient Ignite ignite;

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                while (cacheUpdate.get())
                    for (int i = 0; i < CACHE_COUNT; ++i) {
                        IgniteCache<Long, byte[]> cache = ignite.cache(CACHE_PREFIX + i);
                        cache.invoke(0L, EntryUpdater.INSTANCE);
                    }
            }
            catch (Throwable ex) {
                OutOfMemoryException e = findOutOfMemoryException(ex);
                if (e != null)
                    setOutOfMemoryEx(e);
                else
                    ignite.log().error("CacheUpdater failed", ex);
            }
        }

        /**  */
        private OutOfMemoryException findOutOfMemoryException(Throwable ex) {
            if (ex instanceof OutOfMemoryException)
                return (OutOfMemoryException)ex;
            for (Throwable s : ex.getSuppressed()) {
                OutOfMemoryException e = findOutOfMemoryException(s);
                if (e != null)
                    return e;
            }
            if (ex.getCause() != null) {
                OutOfMemoryException e = findOutOfMemoryException(ex.getCause());
                if (e != null)
                    return e;
            }
            return null;
        }
    }

}
