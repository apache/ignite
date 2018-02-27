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

package org.apache.ignite.internal;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.EventType;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.cache.query.continuous.CacheContinuousQueryManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestClassLoader;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;

/**
 * Test marshaller context.
 */
public class MarshallerContextLockingSelfTest extends GridCommonAbstractTest {
    /** Inner logger. */
    private InnerLogger innerLog = null;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        innerLog = new InnerLogger();

        log = innerLog;
    }

    /**
     * Mumtithread test, used custom class loader
     */
    public void testMultithreadedUpdate() throws Exception {
        multithreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                GridTestClassLoader classLoader = new GridTestClassLoader(
                    InternalExecutor.class.getName(),
                    MarshallerContextImpl.class.getName(),
                    MarshallerContextImpl.ContinuousQueryListener.class.getName()
                );

                Thread.currentThread().setContextClassLoader(classLoader);

                Class clazz = classLoader.loadClass(InternalExecutor.class.getName());

                Object internelExecutor = clazz.newInstance();

                clazz.getMethod("executeTest", GridTestLog4jLogger.class).invoke(internelExecutor, log);

                return null;
            }
        }, 4);

        assertTrue(InternalExecutor.counter.get() == 0);

        assertTrue(innerLog.contains("File already locked"));

        assertTrue(!innerLog.contains("Exception"));
    }

    /**
     * Internal test executor
     */
    public static class InternalExecutor {
        /** Counter. */
        public static AtomicInteger counter = new AtomicInteger();

        /**
        * Executes onUpdated
        */
        public void executeTest(GridTestLog4jLogger log) throws Exception {
            counter.incrementAndGet();

            File workDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", false);

            final MarshallerContextImpl.ContinuousQueryListener queryListener = new MarshallerContextImpl.ContinuousQueryListener(log, workDir);

            final ArrayList evts = new ArrayList<CacheEntryEvent<Integer, String>>();

            IgniteCacheProxy cache = new IgniteCacheProxy();

            evts.add(new CacheContinuousQueryManager.CacheEntryEventImpl(cache, EventType.CREATED, 1, String.class.getName()));

            for (int i = 0; i < 100; i++)
                queryListener.onUpdated(evts);
        }
    }

    /**
     * Internal logger
     */
    public static class InnerLogger extends GridTestLog4jLogger {
        /** */
        private Collection<String> logs = new ConcurrentLinkedDeque<>();

        /**
         * Returns true if and only if this string contains the specified
         * sequence of char values.
         *
         * @param str String.
         */
        public boolean contains(String str) {
            for (String text : logs)
                if (text != null && text.contains(str))
                    return true;

            return false;
        }

        /** {@inheritDoc} */
        @Override public void debug(String msg) {
            logs.add(msg);
        }

        /** {@inheritDoc} */
        @Override public boolean isDebugEnabled() {
            return true;
        }
    }
}
