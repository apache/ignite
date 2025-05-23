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
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Multi-JVM test for dynamic proxy serialization.
 */
public class DynamicProxySerializationMultiJvmSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBinaryMarshaller() throws Exception {
        doTestMarshaller();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testToBinary() throws Exception {
        Ignite ignite = startGrid(0);

        MyProxy p = create();

        MyProxy p0 = ignite.binary().toBinary(p);

        assertSame(p, p0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBinaryField() throws Exception {
        Ignite ignite = startGrids(2);

        BinaryObject bo = ignite.binary().builder("ProxyWrapper").setField("proxy", create()).build();

        int val = ignite.compute(ignite.cluster().forRemotes()).call(new FieldTestCallable(bo));

        assertEquals(42, val);
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestMarshaller() throws Exception {
        Ignite ignite = startGrids(2);

        int val = ignite.compute(ignite.cluster().forRemotes()).call(new MarshallerTestCallable(create()));

        assertEquals(42, val);
    }

    /**
     * @return New proxy.
     */
    private static MyProxy create() {
        return (MyProxy)Proxy.newProxyInstance(DynamicProxySerializationMultiJvmSelfTest.class.getClassLoader(),
            new Class[] { MyProxy.class }, new InvocationHandler() {
                @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                    if ("value".equals(method.getName()))
                        return 42;

                    throw new IllegalStateException();
                }
            });
    }

    /**
     */
    private static class MarshallerTestCallable implements IgniteCallable<Integer> {
        /** */
        private final MyProxy p;

        /**
         * @param p Proxy.
         */
        public MarshallerTestCallable(MyProxy p) {
            this.p = p;
        }

        /** {@inheritDoc} */
        @Override public Integer call() throws Exception {
            return p.value();
        }
    }

    /**
     */
    private static class FieldTestCallable implements IgniteCallable<Integer> {
        /** */
        private final BinaryObject bo;

        /** */
        public FieldTestCallable(BinaryObject bo) {
            this.bo = bo;
        }

        /** {@inheritDoc} */
        @Override public Integer call() throws Exception {
            return bo.<MyProxy>field("proxy").value();
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
