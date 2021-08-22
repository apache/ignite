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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.AbstractNodeNameAwareMarshaller;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for local Ignite instance processing during serialization/deserialization.
 */
public class GridLocalIgniteSerializationTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "cache_name";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName != null && igniteInstanceName.startsWith("binary"))
            cfg.setMarshaller(new BinaryMarshaller());

        return cfg;
    }

    /**
     * Test that calling {@link Ignition#localIgnite()}
     * is safe for binary marshaller.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPutGetSimple() throws Exception {
        checkPutGet(new SimpleTestObject("one"), null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutGetSerializable() throws Exception {
        checkPutGet(new SerializableTestObject("test"), null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutGetExternalizable() throws Exception {
        checkPutGet(new ExternalizableTestObject("test"), null);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutGetBinarylizable() throws Exception {
        checkPutGet(new BinarylizableTestObject("test"), "binaryIgnite");
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPutGet(final TestObject obj, final String igniteInstanceName) throws Exception {

        // Run async to emulate user thread.
        GridTestUtils.runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                try (final Ignite ignite = startGrid(igniteInstanceName)) {
                    if (ignite.configuration().getMarshaller() instanceof AbstractNodeNameAwareMarshaller) {
                        final IgniteCache<Integer, TestObject> cache = ignite.getOrCreateCache(CACHE_NAME);

                        assertNull(obj.ignite());

                        cache.put(1, obj);

                        assertNotNull(obj.ignite());

                        final TestObject loadedObj = cache.get(1);

                        assertNotNull(loadedObj.ignite());

                        assertEquals(obj, loadedObj);
                    }
                }

                return null;
            }
        }).get();
    }

    /**
     *
     */
    private interface TestObject {
        /**
         * @return Ignite instance.
         */
        Ignite ignite();
    }

    /**
     * Test object.
     */
    private static class SimpleTestObject implements TestObject {
        /** */
        private final String val;

        /** */
        private transient Ignite ignite;

        /** */
        private SimpleTestObject(final String val) {
            this.val = val;
        }

        /**
         * @return Object.
         */
        @SuppressWarnings("unused")
        private Object readResolve() {
            ignite = Ignition.localIgnite();

            return this;
        }

        /**
         * @return Object.
         */
        @SuppressWarnings("unused")
        private Object writeReplace() {
            ignite = Ignition.localIgnite();

            return this;
        }

        /** */
        @Override public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final SimpleTestObject simpleTestObj = (SimpleTestObject) o;

            return val != null ? val.equals(simpleTestObj.val) : simpleTestObj.val == null;

        }

        /** */
        @Override public int hashCode() {
            return val != null ? val.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override public Ignite ignite() {
            return ignite;
        }
    }

    /**
     *
     */
    private static class SerializableTestObject implements Serializable, TestObject {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private String val;

        /** */
        private transient Ignite ignite;

        /**
         *
         */
        public SerializableTestObject() {
        }

        /**
         * @param val Value
         */
        public SerializableTestObject(final String val) {
            this.val = val;
        }

        /**
         * @param out Object output.
         * @throws IOException If fail.
         */
        private void writeObject(ObjectOutputStream out) throws IOException {
            U.writeString(out, val);

            ignite = Ignition.localIgnite();
        }

        /**
         * @param in Object input.
         * @throws IOException If fail.
         */
        private void readObject(ObjectInputStream in) throws IOException {
            val = U.readString(in);

            ignite = Ignition.localIgnite();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final SerializableTestObject that = (SerializableTestObject) o;

            return val != null ? val.equals(that.val) : that.val == null;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val != null ? val.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override public Ignite ignite() {
            return ignite;
        }
    }

    /**
     *
     */
    private static class ExternalizableTestObject implements Externalizable, TestObject {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private String val;

        /** */
        private transient Ignite ignite;

        /**
         *
         */
        public ExternalizableTestObject() {
        }

        /**
         * @param val Value.
         */
        public ExternalizableTestObject(final String val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(final ObjectOutput out) throws IOException {
            U.writeString(out, val);

            ignite = Ignition.localIgnite();
        }

        /** {@inheritDoc} */
        @Override public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
            val = U.readString(in);

            ignite = Ignition.localIgnite();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final ExternalizableTestObject that = (ExternalizableTestObject) o;

            return val != null ? val.equals(that.val) : that.val == null;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val != null ? val.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override public Ignite ignite() {
            return ignite;
        }
    }

    /**
     *
     */
    private static class BinarylizableTestObject implements Binarylizable, TestObject {
        /** */
        private String val;

        /** */
        private transient Ignite ignite;

        /**
         *
         */
        public BinarylizableTestObject() {
        }

        /**
         * @param val Value.
         */
        public BinarylizableTestObject(final String val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(final BinaryWriter writer) throws BinaryObjectException {
            writer.rawWriter().writeString(val);

            ignite = Ignition.localIgnite();
        }

        /** {@inheritDoc} */
        @Override public void readBinary(final BinaryReader reader) throws BinaryObjectException {
            val = reader.rawReader().readString();

            ignite = Ignition.localIgnite();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final BinarylizableTestObject that = (BinarylizableTestObject) o;

            return val != null ? val.equals(that.val) : that.val == null;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val != null ? val.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override public Ignite ignite() {
            return ignite;
        }
    }
}
