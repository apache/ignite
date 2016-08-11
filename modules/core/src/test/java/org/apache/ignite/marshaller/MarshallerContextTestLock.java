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

package org.apache.ignite.marshaller;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.EventType;
import org.apache.ignite.internal.MarshallerContextImpl;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;
import org.jetbrains.annotations.Nullable;

import static org.junit.Assert.fail;

/**
 * Test marshaller context.
 */
public class MarshallerContextTestLock extends GridCommonAbstractTest {
    /** Inner logger. */
    InnerLogger innerLog = null;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        innerLog = new InnerLogger();

        log = innerLog;
    }

    /**
     * Execute method onUpdated for emulate file locking
     */
    public void executeTest() throws Exception {
        File workDir = U.resolveWorkDirectory("marshaller", false);

        final MarshallerContextImpl.ContinuousQueryListener qryLsnr = new MarshallerContextImpl.ContinuousQueryListener(log, workDir);

        final ArrayList evts = new ArrayList<CacheEntryEvent<Integer, String>>();

        IgniteCacheProxy cache = new IgniteCacheProxy();

        for(int ind = 0; ind < 2; ind++)
            evts.add(new CacheEntryEventTest(cache, EventType.CREATED, ind, String.class.getName()));

        for (int ind = 0; ind < 100; ind++)
            qryLsnr.onUpdated(evts);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreadedUpdate() throws Exception {

        multithreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {

//                GridTestClassLoader classLoader = new GridTestClassLoader(
//                    MarshallerContextTestLock.class.getName(),
//                    CacheEntryEventTest.class.getName());
//
//                Thread.currentThread().setContextClassLoader(classLoader);

                executeTest();

                return null;
            }
        }, 4);

        assertTrue(!innerLog.contains("Exception"));

        // Shoul be 0 because static counter increment lodaded by custom class loader
        int counter = MarshallerContextImpl.ContinuousQueryListener.counter.get();

        //assertTrue(counter == 0);
    }

    /**
     */
    private static class InnerLogger extends GridTestLog4jLogger {
        /** */
        private List<String> logs = new ArrayList<>();

        /**
         * Returns true if and only if this string contains the specified
         * sequence of char values.
         *
         * @param str String. s
         */
        public boolean contains(String str) {

            for(String text: logs) {
                if (text != null && text.contains(str))
                    return true;
            }
            return false;
        }

        /** {@inheritDoc} */
        @Override public void error(String msg) {
            super.error(msg);
            logs.add(msg);
        }

        /** {@inheritDoc} */
        @Override public void error(String msg, @Nullable Throwable e) {
            super.error(msg, e);
            logs.add(msg);
        }
    }

    private static class CacheEntryEventTest<K, V> extends CacheEntryEvent {
        K key = null;

        V value = null;

        /** {@inheritDoc} */
        public CacheEntryEventTest(Cache source, EventType eventType, K key, V value) {
            super(source, eventType);

            this.key = key;

            this.value = value;
        }

        /** {@inheritDoc} */
        @Override public Object getOldValue() {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean isOldValueAvailable() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public K getKey() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public V getValue() {
            return value;
        }

        /** {@inheritDoc} */
        @Override public Object unwrap(Class aClass) {
            return null;
        }
    }
}