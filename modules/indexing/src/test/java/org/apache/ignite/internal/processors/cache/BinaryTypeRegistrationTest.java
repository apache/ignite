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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.processors.cache.binary.MetadataUpdateProposedMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.BinaryTypeRegistrationTest.TypeRegistrator.DEFAULT_BINARY_FIELD_NAME;

/**
 *
 */
public class BinaryTypeRegistrationTest extends GridCommonAbstractTest {
    /** Holder of sent custom messages. */
    private final ConcurrentLinkedQueue<Object> metadataUpdateProposedMessages = new ConcurrentLinkedQueue<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi() {
            @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
                if (msg instanceof CustomMessageWrapper
                    && ((CustomMessageWrapper)msg).delegate() instanceof MetadataUpdateProposedMessage)
                    metadataUpdateProposedMessages.add(((CustomMessageWrapper)msg).delegate());

                super.sendCustomEvent(msg);
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();

        metadataUpdateProposedMessages.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();

        metadataUpdateProposedMessages.clear();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void shouldSendOnlyOneMetadataMessage() throws Exception {
        Ignite ignite = startGrid(0);

        int threadsNum = 20;

        ExecutorService exec = Executors.newFixedThreadPool(threadsNum);

        CyclicBarrier barrier = new CyclicBarrier(threadsNum + 1);

        for (int i = 0; i < threadsNum; i++)
            exec.submit(new TypeRegistrator(ignite, barrier));

        barrier.await();

        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.SECONDS);

        assertEquals(metadataUpdateProposedMessages.toString(), 1, metadataUpdateProposedMessages.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void shouldSendMetadataMessagePerEachNewBinaryData() throws Exception {
        Ignite ignite = startGrid(0);

        int threadsNum = 20;

        ExecutorService exec = Executors.newFixedThreadPool(threadsNum);

        CyclicBarrier barrier = new CyclicBarrier(threadsNum + 1);

        for (int i = 0; i < threadsNum; i++)
            exec.submit(new TypeRegistrator(ignite, barrier, DEFAULT_BINARY_FIELD_NAME + i));

        barrier.await();

        exec.shutdown();
        exec.awaitTermination(10, TimeUnit.SECONDS);

        assertEquals(threadsNum, metadataUpdateProposedMessages.size());
    }

    /**
     * Register binary type.
     *
     * @param ignite Ignate instance.
     * @param fieldName Field name of new object.
     */
    private static void register(Ignite ignite, String fieldName) {
        IgniteBinary binary = ignite.binary();

        BinaryObjectBuilder builder = binary.builder("TestType");

        builder.setField(fieldName, 1);

        builder.build();
    }

    /**
     * Thread for binary type registration.
     */
    static class TypeRegistrator implements Runnable {
        /** */
        static final String DEFAULT_BINARY_FIELD_NAME = "intField";

        /** */
        private Ignite ignite;

        /** Barrier for synchronous start of all threads. */
        private CyclicBarrier cyclicBarrier;

        /** Binary field name for new binary object. */
        private String binaryFieldName;

        /**
         * @param ignite Ignite instance.
         * @param cyclicBarrier Barrier for synchronous start of all threads.
         */
        TypeRegistrator(Ignite ignite, CyclicBarrier cyclicBarrier) {
            this(ignite, cyclicBarrier, DEFAULT_BINARY_FIELD_NAME);
        }

        /**
         * @param ignite Ignite instance.
         * @param cyclicBarrier Barrier for synchronous start of all threads.
         * @param binaryFieldName Binary field name for new binary object.
         */
        public TypeRegistrator(Ignite ignite, CyclicBarrier cyclicBarrier, String binaryFieldName) {
            this.ignite = ignite;
            this.cyclicBarrier = cyclicBarrier;
            this.binaryFieldName = binaryFieldName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                cyclicBarrier.await();

                register(ignite, binaryFieldName);
            }
            catch (Exception e) {
                log.error("ERROR", e);
            }
        }
    }

}
