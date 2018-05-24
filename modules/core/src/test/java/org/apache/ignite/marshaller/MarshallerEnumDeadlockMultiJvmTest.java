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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Contains test of Enum marshalling with various {@link Marshaller}s. See IGNITE-8547 for details.
 */
public class MarshallerEnumDeadlockMultiJvmTest extends GridCommonAbstractTest {
    /** */
    private Factory<Marshaller> marshFactory;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        return super.getConfiguration(instanceName).setMarshaller(marshFactory.create());
    }

    /** */
    public void testJdkMarshaller() throws Exception {
        marshFactory = new JdkMarshallerFactory();

        runRemoteUnmarshal();
    }

    /** */
    public void testOptimizedMarshaller() throws Exception {
        marshFactory = new OptimizedMarshallerFactory();

        runRemoteUnmarshal();
    }

    /** */
    public void testBinaryMarshaller() throws Exception {
        marshFactory = new BinaryMarshallerFactory();

        runRemoteUnmarshal();
    }

    /** */
    private void runRemoteUnmarshal() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            byte[] one = ignite.configuration().getMarshaller().marshal(DeclaredBodyEnum.ONE);
            byte[] two = ignite.configuration().getMarshaller().marshal(DeclaredBodyEnum.TWO);

            startGrid(1);

            ignite.compute(ignite.cluster().forRemotes()).call(new UnmarshalCallable(one, two));
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** */
    private static class OptimizedMarshallerFactory implements Factory<Marshaller> {
        @Override public Marshaller create() {
            return new OptimizedMarshaller(false);
        }
    }

    /** */
    private static class BinaryMarshallerFactory implements Factory<Marshaller> {
        @Override public Marshaller create() {
            return new BinaryMarshaller();
        }
    }

    /** */
    private static class JdkMarshallerFactory implements Factory<Marshaller> {
        @Override public Marshaller create() {
            return new JdkMarshaller();
        }
    }

    /**
     * Attempts to unmarshal both in-built and inner-class enum values at exactly the same time in multiple threads.
     */
    private static class UnmarshalCallable implements IgniteCallable<Object> {
        /** */
        private final byte[] one;
        /** */
        private final byte[] two;
        /** */
        @IgniteInstanceResource
        private Ignite ign;

        /** */
        public UnmarshalCallable(byte[] one, byte[] two) {
            this.one = one;
            this.two = two;
        }

        /** {@inheritDoc} */
        @Override public Object call() throws Exception {
            List<Thread> threadList = new ArrayList<>();

            final CyclicBarrier b1 = new CyclicBarrier(4);
            final CyclicBarrier b2 = new CyclicBarrier(5);

            for (int i = 0; i < 4; i++) {
                final int ii = i;
                Thread thread = new Thread(new Runnable() {
                    @Override public void run() {
                        try {
                            b1.await();

                            if (ii % 2 == 0)
                                ign.configuration().getMarshaller().unmarshal(one, Thread.currentThread().getContextClassLoader());
                            else
                                ign.configuration().getMarshaller().unmarshal(two, Thread.currentThread().getContextClassLoader());

                            b2.await();
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });

                thread.start();

                threadList.add(thread);
            }

            try {
                b2.await(5, TimeUnit.SECONDS);
            }
            catch (TimeoutException te) {
                throw new IllegalStateException("Failed to wait for completion", te);
            }

            for (Thread thread : threadList)
                thread.join();

            return null;
        }
    }

    /** */
    public enum DeclaredBodyEnum {
        ONE,
        TWO {
            /** {@inheritDoc} */
            @Override public boolean isSupported() {
                return false;
            }
        };

        /**
         * Delays initialization slightly to increase chance of catching race condition.
         */
        DeclaredBodyEnum() {
            AtomicInteger cnt = new AtomicInteger();

            while (cnt.incrementAndGet() < 1000000) { }
        }

        /**
         * A bogus method.
         */
        public boolean isSupported() {
            return true;
        }
    }
}
