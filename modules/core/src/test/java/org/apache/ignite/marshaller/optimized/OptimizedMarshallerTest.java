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

package org.apache.ignite.marshaller.optimized;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.GridMarshallerTestInheritedBean;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAdapter;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class OptimizedMarshallerTest extends GridCommonAbstractTest {
    /**
     * @return Marshaller.
     */
    private OptimizedMarshaller marshaller() {
        U.clearClassCache();

        OptimizedMarshaller marsh = new OptimizedMarshaller();

        marsh.setContext(new MarshallerContextTestImpl());

        return marsh;
    }

    /**
     * Tests ability to marshal non-serializable objects.
     *
     * @throws IgniteCheckedException If marshalling failed.
     */
    public void testNonSerializable() throws IgniteCheckedException {
        OptimizedMarshaller marsh = marshaller();

        marsh.setRequireSerializable(false);

        NonSerializable outObj = marsh.unmarshal(marsh.marshal(new NonSerializable(null)), null);

        outObj.checkAfterUnmarshalled();
    }

    /**
     * Tests ability to marshal non-serializable objects.
     *
     * @throws IgniteCheckedException If marshalling failed.
     */
    public void testNonSerializable1() throws IgniteCheckedException {
        OptimizedMarshaller marsh = marshaller();

        marsh.setRequireSerializable(false);

        byte[] bytes = marsh.marshal(new TcpDiscoveryVmIpFinder());

        TcpDiscoveryIpFinder ipFinder = marsh.unmarshal(bytes, null);

        assertFalse(ipFinder.isShared());

        ipFinder = marsh.unmarshal(marsh.marshal(new TcpDiscoveryVmIpFinder(true)), null);

        assertTrue(ipFinder.isShared());
    }

    /**
     * Tests ability to marshal non-serializable objects.
     *
     * @throws IgniteCheckedException If marshalling failed.
     */
    public void testNonSerializable2() throws IgniteCheckedException {
        OptimizedMarshaller marsh = marshaller();

        marsh.setRequireSerializable(false);

        TcpDiscoveryIpFinderAdapter ipFinder = new TcpDiscoveryIpFinderAdapter() {
            @Override public Collection<InetSocketAddress> getRegisteredAddresses() {
                return null;
            }

            @Override public void registerAddresses(Collection<InetSocketAddress> addrs) {
                //No-op.
            }

            @Override public void unregisterAddresses(Collection<InetSocketAddress> addrs) {
                //No-op.
            }
        };

        ipFinder.setShared(false);

        byte[] bytes = marsh.marshal(ipFinder);

        ipFinder = marsh.unmarshal(bytes, null);

        assertFalse(ipFinder.isShared());
    }

    /**
     * Tests ability to marshal non-serializable objects.
     *
     * @throws IgniteCheckedException If marshalling failed.
     */
    public void testNonSerializable3() throws IgniteCheckedException {
        OptimizedMarshaller marsh = marshaller();

        marsh.setRequireSerializable(false);

        byte[] bytes = marsh.marshal(new TestTcpDiscoveryIpFinderAdapter());

        TcpDiscoveryIpFinder ipFinder = marsh.unmarshal(bytes, null);

        assertFalse(ipFinder.isShared());
    }

     /**
     * Tests ability to marshal non-serializable objects.
     *
     * @throws IgniteCheckedException If marshalling failed.
     */
    public void testNonSerializable4() throws IgniteCheckedException {
        OptimizedMarshaller marsh = marshaller();

        marsh.setRequireSerializable(false);

        byte[] bytes = marsh.marshal(new GridMarshallerTestInheritedBean());

        info(Arrays.toString(bytes));

        GridMarshallerTestInheritedBean bean = marsh.unmarshal(bytes, null);

        assertTrue(bean.isFlag());
    }

     /**
     * Tests ability to marshal non-serializable objects.
     *
     * @throws IgniteCheckedException If marshalling failed.
     */
    public void testNonSerializable5() throws IgniteCheckedException {
        Marshaller marsh = marshaller();

        byte[] bytes = marsh.marshal(true);

        Boolean val = marsh.unmarshal(bytes, null);

        assertTrue(val);
    }

    /**
     * Tests ability to marshal serializable objects.
     *
     * @throws IgniteCheckedException If marshalling failed.
     */
    public void testSerializable() throws IgniteCheckedException {
        Marshaller marsh = marshaller();

        SomeSerializable outObj = marsh.unmarshal(marsh.marshal(new SomeSerializable(null)), null);

        outObj.checkAfterUnmarshalled();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testSerializableAfterChangingValue() throws IgniteCheckedException {
        Marshaller marsh = marshaller();

        SomeSimpleSerializable newObj = new SomeSimpleSerializable();

        assert(newObj.flag);

        newObj.setFlagValue(false);

        assert(! newObj.flag);

        SomeSimpleSerializable outObj = marsh.unmarshal(marsh.marshal(newObj), null);

        assert (! outObj.flag);
    }

    /**
     * Tests ability to marshal externalizable objects.
     *
     * @throws IgniteCheckedException If marshalling failed.
     */
    public void testExternalizable() throws IgniteCheckedException {
        Marshaller marsh = marshaller();

        ExternalizableA outObj = marsh.unmarshal(marsh.marshal(new ExternalizableA(null, true)), null);
        ExternalizableA outObj1 = marsh.unmarshal(marsh.marshal(new ExternalizableA(null, false)), null);

        assertNotNull(outObj);
        assertNotNull(outObj1);
    }

    /**
     * Tests {@link OptimizedMarshaller#setRequireSerializable(boolean)}.
     */
    public void testRequireSerializable() {
        OptimizedMarshaller marsh = marshaller();

        marsh.setRequireSerializable(true);

        try {
            marsh.marshal(new NonSerializable(null));

            fail();
        }
        catch (IgniteCheckedException ignore) {
            // No-op.
        }
    }

    /**
     * Tests {@link Proxy}.
     *
     * @throws IgniteCheckedException If marshalling failed.
     */
    public void testProxy() throws IgniteCheckedException {
        OptimizedMarshaller marsh = marshaller();

        marsh.setRequireSerializable(false);

        SomeItf inItf = (SomeItf)Proxy.newProxyInstance(
            OptimizedMarshallerTest.class.getClassLoader(), new Class[] {SomeItf.class},
            new InvocationHandler() {
                private NonSerializable obj = new NonSerializable(null);

                @Override public Object invoke(Object proxy, Method mtd, Object[] args) throws Throwable {
                    obj.checkAfterUnmarshalled();

                    return 17;
                }
            }
        );

        SomeItf outItf = marsh.unmarshal(marsh.marshal(inItf), null);

        assertEquals(outItf.checkAfterUnmarshalled(), 17);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDescriptorCache() throws Exception {
        try {
            Ignite ignite = startGridsMultiThreaded(2);

            String taskClsName = "org.apache.ignite.tests.p2p.SingleSplitTestTask";
            String jobClsName = "org.apache.ignite.tests.p2p.SingleSplitTestTask$SingleSplitTestJob";

            ClassLoader ldr = getExternalClassLoader();

            Class<? extends ComputeTask<?, ?>> taskCls = (Class<? extends ComputeTask<?, ?>>)ldr.loadClass(taskClsName);
            Class<? extends ComputeTask<?, ?>> jobCls = (Class<? extends ComputeTask<?, ?>>)ldr.loadClass(jobClsName);

            ignite.compute().localDeployTask(taskCls, ldr);

            ignite.compute().execute(taskClsName, 2);

            ConcurrentMap<Class<?>, OptimizedClassDescriptor> cache =
                U.field(ignite.configuration().getMarshaller(), "clsMap");

            assertTrue(cache.containsKey(jobCls));

            ignite.compute().undeployTask(taskClsName);

            // Wait for undeploy.
            Thread.sleep(1000);

            assertFalse(cache.containsKey(jobCls));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPerformance() throws Exception {
        System.gc();

        checkPerformance(10000, 4);
    }

    /**
     * @param cnt Number of marshalling attempts.
     * @param tries Number of retries.
     * @throws Exception If failed.
     */
    private void checkPerformance(int cnt, int tries) throws Exception {
        Marshaller marsh = marshaller();

        for (int j = 0; j < tries; j++) {
            System.gc();

            long start = System.currentTimeMillis();

            for (int i = 0; i < cnt; i++) {
                TestCacheKey key = new TestCacheKey("key", "id");

                TestCacheKey outKey = marsh.unmarshal(marsh.marshal(key), null);

                assert key.equals(outKey);
                assert key.hashCode() == outKey.hashCode();
            }

            info("Time non-serializable: " + (System.currentTimeMillis() - start));

            System.gc();

            start = System.currentTimeMillis();

            for (int i = 0; i < cnt; i++) {
                TestCacheKeySerializable key1 = new TestCacheKeySerializable("key", "id");

                TestCacheKeySerializable outKey = marsh.unmarshal(marsh.marshal(key1), null);

                assert key1.equals(outKey);
                assert key1.hashCode() == outKey.hashCode();
            }

            info("Time serializable: " + (System.currentTimeMillis() - start));

            System.gc();

            start = System.currentTimeMillis();

            for (int i = 0; i < cnt; i++) {
                TestCacheKeyExternalizable key2 = new TestCacheKeyExternalizable("key", "id");

                TestCacheKeyExternalizable outKey = marsh.unmarshal(marsh.marshal(key2), null);

                assert key2.equals(outKey);
                assert key2.hashCode() == outKey.hashCode();
            }

            info("Time externalizable: " + (System.currentTimeMillis() - start));

            info(">>>");
        }

        info(">>> Finished performance check <<<");
    }

    /**
     * Some non-serializable class.
     */
    @SuppressWarnings( {"PublicField","TransientFieldInNonSerializableClass","FieldMayBeStatic"})
    private static class NonSerializableA {
        /** */
        private final long longVal = 0x33445566778899AAL;

        /** */
        protected Short shortVal = (short)0xAABB;

        /** */
        public String[] strArr = {"AA","BB"};

        /** */
        public boolean flag1 = true;

        /** */
        public boolean flag2;

        /** */
        public Boolean flag3;

        /** */
        public Boolean flag4 = true;

        /** */
        public Boolean flag5 = false;

        /** */
        private transient int intVal = 0xAABBCCDD;

        /**
         * @param strArr Array.
         * @param shortVal Short value.
         */
        @SuppressWarnings( {"UnusedDeclaration"})
        private NonSerializableA(@Nullable String[] strArr, @Nullable Short shortVal) {
            // No-op.
        }

        /**
         * Checks correctness of the state after unmarshalling.
         */
        void checkAfterUnmarshalled() {
            assertEquals(longVal, 0x33445566778899AAL);

            assertEquals(shortVal.shortValue(), (short)0xAABB);

            assertTrue(Arrays.equals(strArr, new String[] {"AA","BB"}));

            assertEquals(intVal, 0);

            assertTrue(flag1);
            assertFalse(flag2);
            assertNull(flag3);
            assertTrue(flag4);
            assertFalse(flag5);
        }
    }

    /**
     * Some non-serializable class.
     */
    @SuppressWarnings( {"PublicField","TransientFieldInNonSerializableClass","PackageVisibleInnerClass"})
    static class NonSerializableB extends NonSerializableA {
        /** */
        public Short shortVal = 0x1122;

        /** */
        public long longVal = 0x8877665544332211L;

        /** */
        private transient NonSerializableA[] aArr = {
            new NonSerializableA(null, null),
            new NonSerializableA(null, null),
            new NonSerializableA(null, null)
        };

        /** */
        protected Double doubleVal = 123.456;

        /**
         * Just to eliminate the default constructor.
         */
        private NonSerializableB() {
            super(null, null);
        }

        /**
         * Checks correctness of the state after unmarshalling.
         */
        @Override void checkAfterUnmarshalled() {
            super.checkAfterUnmarshalled();

            assertEquals(shortVal.shortValue(), 0x1122);

            assertEquals(longVal, 0x8877665544332211L);

            assertNull(aArr);

            assertEquals(doubleVal, 123.456);
        }
    }

    /**
     * Some non-serializable class.
     */
    @SuppressWarnings( {"TransientFieldInNonSerializableClass","PublicField"})
    private static class NonSerializable extends NonSerializableB {
        /** */
        private int idVal = -17;

        /** */
        private final NonSerializableA aVal = new NonSerializableB();

        /** */
        private transient NonSerializableB bVal = new NonSerializableB();

        /** */
        private NonSerializableA[] bArr = new NonSerializableA[] {
            new NonSerializableB(),
            new NonSerializableA(null, null)
        };

        /** */
        public float floatVal = 567.89F;

        /**
         * Just to eliminate the default constructor.
         *
         * @param aVal Unused.
         */
        @SuppressWarnings( {"UnusedDeclaration"})
        private NonSerializable(NonSerializableA aVal) {
        }

        /**
         * Checks correctness of the state after unmarshalling.
         */
        @Override void checkAfterUnmarshalled() {
            super.checkAfterUnmarshalled();

            assertEquals(idVal, -17);

            aVal.checkAfterUnmarshalled();

            assertNull(bVal);

            for (NonSerializableA a : bArr) {
                a.checkAfterUnmarshalled();
            }

            assertEquals(floatVal, 567.89F);
        }
    }

    /**
     * Some serializable class.
     */
    @SuppressWarnings( {"PublicField","TransientFieldInNonSerializableClass","PackageVisibleInnerClass"})
    static class ForSerializableB {
        /** */
        public Short shortVal = 0x1122;

        /** */
        public long longVal = 0x8877665544332211L;

        /** */
        private transient NonSerializableA[] aArr;

        /** */
        private transient String strVal = "abc";

        /** */
        protected Double doubleVal = 123.456;

        /**
         */
        protected void init() {
            shortVal = 0x1122;

            longVal = 0x8877665544332211L;

            aArr = new NonSerializableA[] {
                new NonSerializableA(null, null),
                new NonSerializableA(null, null),
                new NonSerializableA(null, null)
            };
        }

        /**
         * Checks correctness of the state after unmarshalling.
         */
        void checkAfterUnmarshalled() {
            assertEquals(shortVal.shortValue(), 0x1122);

            assertEquals(longVal, 0x8877665544332211L);

            assertNull(aArr);

            assertNull(strVal);

            assertEquals(doubleVal, 123.456);
        }
    }

    /**
     * Some serializable class.
     */
    private static class SomeSimpleSerializable extends ComputeJobAdapter {
        /** */
        private boolean flag = true;

        /**
         * @param newFlagVal - The new value of flag field.
         */
        public void setFlagValue(boolean newFlagVal) {
            flag = newFlagVal;
        }

        /** {@inheritDoc} */
        @Override public Object execute() {
            assert false;

            return null;
        }
    }
    /**
     * Some serializable class.
     */
    private static class SomeSerializable extends ForSerializableB implements Serializable {
        /**
         * Just to eliminate the default constructor.
         *
         * @param id Unused.
         */
        @SuppressWarnings( {"UnusedDeclaration"})
        private SomeSerializable(Long id) {
            init();
        }
    }

    /**
     */
    private static interface SomeItf {
        /**
         * @return Check result.
         */
        int checkAfterUnmarshalled();
    }

    /**
     * Some externalizable class.
     */
    @SuppressWarnings( {"UnusedDeclaration", "PublicField"})
    private static class ExternalizableA implements Externalizable {
        /** */
        private boolean boolVal;

        /** */
        public String[] strArr;

        /** No-arg constructor is required by externalization.  */
        public ExternalizableA() {
            // No-op.
        }

        /**
         *
         * @param strArr String array.
         * @param boolVal Boolean value.
         */
        private ExternalizableA(String[] strArr, boolean boolVal) {
            this.strArr = strArr;
            this.boolVal = boolVal;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeBoolean(false);
            out.writeBoolean(false);
            out.writeBoolean(false);
            out.writeBoolean(false);
            out.writeBoolean(false);
            out.writeBoolean(false);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            boolVal = in.readBoolean();
            in.readBoolean();
            in.readBoolean();
            in.readBoolean();
            in.readBoolean();
            in.readBoolean();
        }
    }

    /**
     *
     */
    private static class TestCacheKey implements Serializable {
        /** */
        private String key;

        /** */
        @SuppressWarnings({"UnusedDeclaration"})
        private String terminalId;

        /**
         * @param key Key.
         * @param terminalId Some ID.
         */
        TestCacheKey(String key, String terminalId) {
            this.key = key;
            this.terminalId = terminalId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof TestCacheKey && key.equals(((TestCacheKey)obj).key);
        }
    }

    /**
     *
     */
    private static class TestCacheKeySerializable implements Serializable {
        /** */
        private String key;

        /** */
        @SuppressWarnings({"UnusedDeclaration"})
        private String terminalId;

        /**
         * @param key Key.
         * @param terminalId Some ID.
         */
        TestCacheKeySerializable(String key, String terminalId) {
            this.key = key;
            this.terminalId = terminalId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof TestCacheKeySerializable && key.equals(((TestCacheKeySerializable)obj).key);
        }
    }

    /**
     *
     */
    private static class TestCacheKeyExternalizable implements Externalizable {
        /** */
        private String key;

        /** */
        private String terminalId;

        /**
         *
         */
        public TestCacheKeyExternalizable() {
            // No-op.
        }

        /**
         * @param key Key.
         * @param terminalId Some ID.
         */
        TestCacheKeyExternalizable(String key, String terminalId) {
            this.key = key;
            this.terminalId = terminalId;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key.hashCode();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof TestCacheKeyExternalizable && key.equals(((TestCacheKeyExternalizable)obj).key);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, key);
            U.writeString(out, terminalId);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            key = U.readString(in);
            terminalId = U.readString(in);
        }
    }
}