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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Multi-JVM test for dynamic proxy serialization.
 */
public class DynamicProxySerializationMultiJvmSelfTest extends GridCommonAbstractTest {
    /** */
    private static Callable<Marshaller> marshFactory;

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(marshFactory.call());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimized() throws Exception {
        marshFactory = new Callable<Marshaller>() {
            @Override public Marshaller call() throws Exception {
                return new OptimizedMarshaller(false);
            }
        };

        doTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinary() throws Exception {
        marshFactory = new Callable<Marshaller>() {
            @Override public Marshaller call() throws Exception {
                return new BinaryMarshaller();
            }
        };

        doTest();
    }

    /**
     * @throws Exception If failed.
     */
    private void doTest() throws Exception {
        try {
            Ignite ignite = startGrids(2);

            MyProxy p = (MyProxy)Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class[] { MyProxy.class }, new InvocationHandler() {
                    @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        if ("value".equals(method.getName()))
                            return 42;

                        throw new IllegalStateException();
                    }
                });

            int val = ignite.compute(ignite.cluster().forRemotes()).call(new MyCallable(p));

            assertEquals(42, val);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     */
    private static class MyCallable implements IgniteCallable<Integer> {
        /** */
        private final MyProxy p;

        /**
         * @param p Proxy.
         */
        public MyCallable(MyProxy p) {
            this.p = p;
        }

        /** {@inheritDoc} */
        @Override public Integer call() throws Exception {
            return p.value();
        }
    }

    /**
     */
    private static interface MyProxy {
        /**
         * @return Value.
         */
        public int value();
    }
}
