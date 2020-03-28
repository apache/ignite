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

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

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
    @Test
    public void testJdkMarshaller() throws Exception {
        marshFactory = new JdkMarshallerFactory();

        runRemoteUnmarshal();
    }

    /** */
    @Test
    public void testOptimizedMarshaller() throws Exception {
        marshFactory = new OptimizedMarshallerFactory();

        runRemoteUnmarshal();
    }

    /** */
    @Test
    public void testBinaryMarshaller() throws Exception {
        marshFactory = new BinaryMarshallerFactory();

        runRemoteUnmarshal();
    }

    /** */
    private void runRemoteUnmarshal() throws Exception {
        Ignite ignite = startGrid(0);

        byte[] one = ignite.configuration().getMarshaller().marshal(DeclaredBodyEnum.ONE);
        byte[] two = ignite.configuration().getMarshaller().marshal(DeclaredBodyEnum.TWO);

        startGrid(1);

        ignite.compute(ignite.cluster().forRemotes()).call(new UnmarshalCallable(one, two));
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    private static class OptimizedMarshallerFactory implements Factory<Marshaller> {
        /** {@inheritDoc} */
        @Override public Marshaller create() {
            return new OptimizedMarshaller(false);
        }
    }

    /** */
    private static class BinaryMarshallerFactory implements Factory<Marshaller> {
        /** {@inheritDoc} */
        @Override public Marshaller create() {
            return new BinaryMarshaller();
        }
    }

    /** */
    private static class JdkMarshallerFactory implements Factory<Marshaller> {
        /** {@inheritDoc} */
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
            ExecutorService executor = Executors.newFixedThreadPool(2);

            final CyclicBarrier start = new CyclicBarrier(2);

            for (int i = 0; i < 2; i++) {
                final int ii = i;

                executor.execute(new Runnable() {
                    @Override public void run() {
                        try {
                            start.await();

                            if (ii == 0)
                                ign.configuration().getMarshaller().unmarshal(one, null);
                            else
                                ign.configuration().getMarshaller().unmarshal(two, null);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

            try {
                executor.shutdown();

                executor.awaitTermination(5, TimeUnit.SECONDS);

                if (!executor.isTerminated())
                    throw new IllegalStateException("Failed to wait for completion");
            }
            catch (Exception te) {
                throw new IllegalStateException("Failed to wait for completion", te);
            }

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
         * A bogus method.
         */
        public boolean isSupported() {
            return true;
        }
    }
}
