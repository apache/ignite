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
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class ExternalizableKeyValueTest extends GridCommonAbstractTest {
    /**
     * Key class.
     */
    public static class Key implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @QuerySqlField(index = true)
        protected int key;

        /**
         * Default constructor.
         */
        public Key() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param key the key
         */
        public Key(final int key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Objects.hashCode(key);
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(final Object o) {
            if (o == this)
                return true;

            if (o instanceof Key) {
                final Key that = (Key)o;
                return key == that.key;
            }
            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Key{" +
                "key=" + key +
                '}';
        }
    }

    /**
     * {@code Externalizable} Key class.
     */
    public static class ExternalizableKey extends Key implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Default constructor.
         */
        public ExternalizableKey() {
            // No-op.
        }

        /**
         * Constructor
         *
         * @param key the key
         */
        public ExternalizableKey(int key) {
            super(key);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "ExternalizableKey{" +
                "key=" + key +
                "}";
        }

        /** {@inheritDoc} */
        @Override
        public void writeExternal(final ObjectOutput out)
            throws IOException {
            out.writeInt(key);
        }

        /** {@inheritDoc} */
        @Override
        public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
            key = in.readInt();
        }
    }

    /**
     * Value class.
     */
    public static class Value implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @QuerySqlField(index = true)
        protected Integer val;

        /**
         * Default constructor.
         */
        public Value() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param val Value.
         */
        public Value(Integer val) {
            this.val = val;
        }

        /**
         * Returns value.
         *
         * @return Value.
         */
        public Integer getVal() {
            return val;
        }

        /**
         * Sets value.
         *
         * @param val Value.
         */
        public void setVal(Integer val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Value{" +
                "val=" + val +
                '}';
        }
    }

    /**
     * {@code Externalizable} Value class.
     */
    public static class ExternalizableValue extends Value implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Default constructor.
         */
        public ExternalizableValue() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param val Value.
         */
        public ExternalizableValue(Integer val) {
            super(val);
        }

        /** {@inheritDoc} */
        @Override
        public void writeExternal(final ObjectOutput out)
            throws IOException {
            out.writeInt(val);
        }

        /** {@inheritDoc} */
        @Override
        public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
            setVal(in.readInt());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "ExternalizableValue{" +
                "val=" + val +
                "} ";
        }
    }

    /**
     * Marshaller type.
     */
    public enum MarshallerType {
        /** */
        JDK,
        /** */
        OPTIMIZED,
        /** */
        BINARY
    }

    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "cacheWithExternalizedKeyAndOrValue";

    /**
     * Returns a marshaller for the specified type.
     *
     * @param marshallerType Marshaller type.
     * @return Marshaller. {@code null} if {@link MarshallerType#BINARY} type
     * is specified or type is unknown.
     */
    private static Marshaller getMarshaller(MarshallerType marshallerType) {
        switch (marshallerType) {
            case JDK:
                return new JdkMarshaller();
            case OPTIMIZED:
                return new OptimizedMarshaller();
        }

        // Binary marshaller is used by default.
        return null;
    }

    /** */
    private MarshallerType marshallerType;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(final String gridName) throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration()
            .setGridName(gridName)
            .setPeerClassLoadingEnabled(false)
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        Marshaller marshaller = getMarshaller(marshallerType);

        // By default a binary marshaller is used.
        if (marshaller != null)
            cfg.setMarshaller(marshaller);

        return cfg;
    }

    /**
     * @param keyCls Key class.
     * @param valCls Val class.
     */
    private static <K, V> CacheConfiguration<K, V> cacheConfig(Class<K> keyCls, Class<V> valCls) {
        return new CacheConfiguration<K, V>(CACHE_NAME).setIndexedTypes(keyCls, valCls);
    }

    /** */
    private <K, V> void doSequentialPut(
        MarshallerType marshallerType, Class<K> keyCls, Class<V> valCls, K key1, V val1, K key2, V val2
    ) throws Exception {
        this.marshallerType = marshallerType;

        startGrids(2);

        IgniteCache<K, V> cache = grid(0).createCache(cacheConfig(keyCls, valCls));

        cache.put(key1, val1);
        cache.put(key2, val2);

        assertEquals(2, cache.size());
    }

    /** */
    public void doSequentialPutWithExternalizableKeyAndPrimitiveValue(MarshallerType marshallerType) throws Exception {
        doSequentialPut(
            marshallerType,
            ExternalizableKey.class,
            Integer.class,
            new ExternalizableKey(1),
            1,
            new ExternalizableKey(2),
            2
        );
    }

    /** */
    public void doSequentialPutWithExternalizableKeyAndExternalizableValue(MarshallerType marshallerType) throws Exception {
        doSequentialPut(
            marshallerType,
            ExternalizableKey.class,
            ExternalizableValue.class,
            new ExternalizableKey(1),
            new ExternalizableValue(111),
            new ExternalizableKey(2),
            new ExternalizableValue(222)
        );
    }

    /** */
    public void doSequentialPutWithExternalizableKeyAndPojoValue(MarshallerType marshallerType) throws Exception {
        doSequentialPut(
            marshallerType,
            ExternalizableKey.class,
            Value.class,
            new ExternalizableKey(1),
            new Value(111),
            new ExternalizableKey(2),
            new Value(222)
        );
    }

    /** */
    public void doSequentialPutWithPrimitiveKeyAndExternalizableValue(MarshallerType marshallerType) throws Exception {
        doSequentialPut(
            marshallerType,
            Integer.class,
            ExternalizableValue.class,
            1,
            new ExternalizableValue(111),
            2,
            new ExternalizableValue(222)
        );
    }

    /** */
    public void doSequentialPutWithPojoKeyAndExternalizableValue(MarshallerType marshallerType) throws Exception {
        doSequentialPut(
            marshallerType,
            Key.class,
            ExternalizableValue.class,
            new Key(1),
            new ExternalizableValue(111),
            new Key(2),
            new ExternalizableValue(222)
        );
    }

    /** */
    public void testSequentialPutWithExternalizableKeyAndPrimitiveValueAndBinaryMarshaller() throws Exception {
        doSequentialPutWithExternalizableKeyAndPrimitiveValue(MarshallerType.BINARY);
    }

    /** */
    public void testSequentialPutWithExternalizableKeyAndPrimitiveValueAndJdkMarshaller() throws Exception {
        doSequentialPutWithExternalizableKeyAndPrimitiveValue(MarshallerType.JDK);
    }

    /** */
    public void testSequentialPutWithExternalizableKeyAndPrimitiveValueAndOptimizedMarshaller() throws Exception {
        doSequentialPutWithExternalizableKeyAndPrimitiveValue(MarshallerType.OPTIMIZED);
    }

    /** */
    public void testSequentialPutWithExternalizableKeyAndExternalizableValueAndBinaryMarshaller() throws Exception {
        doSequentialPutWithExternalizableKeyAndExternalizableValue(MarshallerType.BINARY);
    }

    /** */
    public void testSequentialPutWithExternalizableKeyAndExternalizableValueAndJdkMarshaller() throws Exception {
        doSequentialPutWithExternalizableKeyAndExternalizableValue(MarshallerType.JDK);
    }

    /** */
    public void testSequentialPutWithExternalizableKeyAndExternalizableValueAndOptimizedMarshaller() throws Exception {
        doSequentialPutWithExternalizableKeyAndExternalizableValue(MarshallerType.OPTIMIZED);
    }

    /** */
    public void testSequentialPutWithExternalizableKeyAndPojoValueAndBinaryMarshaller() throws Exception {
        doSequentialPutWithExternalizableKeyAndPojoValue(MarshallerType.BINARY);
    }

    /** */
    public void testSequentialPutWithExternalizableKeyAndPojoValueAndJdkMarshaller() throws Exception {
        doSequentialPutWithExternalizableKeyAndPojoValue(MarshallerType.JDK);
    }

    /** */
    public void testSequentialPutWithExternalizableKeyAndPojoValueAndOptimizedMarshaller() throws Exception {
        doSequentialPutWithExternalizableKeyAndPojoValue(MarshallerType.OPTIMIZED);
    }

    /** */
    public void testSequentialPutWithPrimitiveKeyAndExternalizableValueAndBinaryMarshaller() throws Exception {
        doSequentialPutWithPrimitiveKeyAndExternalizableValue(MarshallerType.BINARY);
    }

    /** */
    public void testSequentialPutWithPrimitiveKeyAndExternalizableValueAndJdkMarshaller() throws Exception {
        doSequentialPutWithPrimitiveKeyAndExternalizableValue(MarshallerType.JDK);
    }

    /** */
    public void testSequentialPutWithPrimitiveKeyAndExternalizableValueAndOptimizedMarshaller() throws Exception {
        doSequentialPutWithPrimitiveKeyAndExternalizableValue(MarshallerType.OPTIMIZED);
    }

    /** */
    public void testSequentialPutWithPojoKeyAndExternalizableValueAndBinaryMarshaller() throws Exception {
        doSequentialPutWithPojoKeyAndExternalizableValue(MarshallerType.BINARY);
    }

    /** */
    public void testSequentialPutWithPojoKeyAndExternalizableValueAndJdkMarshaller() throws Exception {
        doSequentialPutWithPojoKeyAndExternalizableValue(MarshallerType.JDK);
    }

    /** */
    public void testSequentialPutWithPojoKeyAndExternalizableValueAndOptimizedMarshaller() throws Exception {
        doSequentialPutWithPojoKeyAndExternalizableValue(MarshallerType.OPTIMIZED);
    }
}
