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

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.closure.GridClosureProcessor;
import org.apache.ignite.internal.processors.marshaller.MarshallerMappingItem;
import org.apache.ignite.internal.processors.marshaller.MarshallerMappingTransport;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.testframework.GridTestClassLoader;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.logger.GridTestLog4jLogger;

import static org.apache.ignite.internal.MarshallerPlatformIds.JAVA_ID;

/**
 * Test marshaller context.
 */
public class MarshallerContextLockingSelfTest extends GridCommonAbstractTest {
    /** Inner logger. */
    private InnerLogger innerLog;

    /** */
    private GridTestKernalContext ctx;

    /** */
    private static final int THREADS = 4;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        innerLog = new InnerLogger();

        IgniteConfiguration iCfg = new IgniteConfiguration();
        iCfg.setClientMode(false);

        ctx = new GridTestKernalContext(innerLog, iCfg) {
            @Override public IgniteLogger log(Class<?> cls) {
                return innerLog;
            }
        };

        ctx.setSystemExecutorService(Executors.newFixedThreadPool(THREADS));

        ctx.add(new PoolProcessor(ctx));
        ctx.add(new GridClosureProcessor(ctx));
    }

    /**
     * Multithreaded test, used custom class loader
     */
    public void testMultithreadedUpdate() throws Exception {
        multithreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                GridTestClassLoader classLoader = new GridTestClassLoader(
                    InternalExecutor.class.getName(),
                    MarshallerContextImpl.class.getName(),
                    MarshallerContextImpl.CombinedMap.class.getName(),
                    MappingStoreTask.class.getName(),
                    MarshallerMappingFileStore.class.getName(),
                    MarshallerMappingTransport.class.getName()
                );

                Thread.currentThread().setContextClassLoader(classLoader);

                Class clazz = classLoader.loadClass(InternalExecutor.class.getName());

                Object internelExecutor = clazz.newInstance();

                clazz.getMethod("executeTest", GridTestLog4jLogger.class, GridKernalContext.class)
                        .invoke(internelExecutor, log, ctx);

                return null;
            }
        }, THREADS);

        final CountDownLatch arrive = new CountDownLatch(THREADS);

        // Wait for all pending tasks in closure processor to complete.
        for (int i = 0; i < THREADS; i++) {
            ctx.closure().runLocalSafe(new GridPlainRunnable() {
                @Override public void run() {
                    arrive.countDown();

                    try {
                        arrive.await();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();

                        throw new IgniteInterruptedException(e);
                    }
                }
            }, true);
        }

        arrive.await();

        assertTrue(InternalExecutor.counter.get() == 0);

        assertTrue(!innerLog.contains("Exception"));

        assertTrue(innerLog.contains("File already locked"));
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
        public void executeTest(GridTestLog4jLogger log, GridKernalContext ctx) throws Exception {
            counter.incrementAndGet();

            MarshallerContextImpl marshallerContext = new MarshallerContextImpl(null);
            marshallerContext.onMarshallerProcessorStarted(ctx, null);

            MarshallerMappingItem item = new MarshallerMappingItem(JAVA_ID, 1, String.class.getName());

            for (int i = 0; i < 400; i++)
                marshallerContext.onMappingAccepted(item);
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
