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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryReflectiveSerializer;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.cache.Cache;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Test for query with BinaryMarshaller and different serialization modes.
 */
public class BinarySerializationQuerySelfTest extends GridCommonAbstractTest {
    /** Ignite instance. */
    private Ignite ignite;

    /** Cache. */
    private IgniteCache<Integer, Object> cache;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));
        cfg.setDiscoverySpi(discoSpi);

        cfg.setMarshaller(new BinaryMarshaller());

        if (useReflectiveSerializer()) {
            BinaryTypeConfiguration binTypCfg1 = new BinaryTypeConfiguration(EntityPlain.class.getName());
            BinaryTypeConfiguration binTypCfg2 = new BinaryTypeConfiguration(EntitySerializable.class.getName());
            BinaryTypeConfiguration binTypCfg3 = new BinaryTypeConfiguration(EntityExternalizable.class.getName());
            BinaryTypeConfiguration binTypCfg4 = new BinaryTypeConfiguration(EntityBinarylizable.class.getName());
            BinaryTypeConfiguration binTypCfg5 = new BinaryTypeConfiguration(EntityWriteReadObject.class.getName());

            binTypCfg1.setSerializer(new BinaryReflectiveSerializer());
            binTypCfg2.setSerializer(new BinaryReflectiveSerializer());
            binTypCfg3.setSerializer(new BinaryReflectiveSerializer());
            binTypCfg4.setSerializer(new BinaryReflectiveSerializer());
            binTypCfg5.setSerializer(new BinaryReflectiveSerializer());

            List<BinaryTypeConfiguration> binTypCfgs = new ArrayList<>();

            binTypCfgs.add(binTypCfg1);
            binTypCfgs.add(binTypCfg2);
            binTypCfgs.add(binTypCfg3);
            binTypCfgs.add(binTypCfg4);
            binTypCfgs.add(binTypCfg5);

            BinaryConfiguration binCfg = new BinaryConfiguration();

            binCfg.setTypeConfigurations(binTypCfgs);

            cfg.setBinaryConfiguration(binCfg);
        }

        CacheConfiguration cacheCfg = new CacheConfiguration();

        cacheCfg.setName(null);
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        List<CacheTypeMetadata> metas = new ArrayList<>();

        metas.add(metaForClass(EntityPlain.class));
        metas.add(metaForClass(EntitySerializable.class));
        metas.add(metaForClass(EntityExternalizable.class));
        metas.add(metaForClass(EntityBinarylizable.class));
        metas.add(metaForClass(EntityWriteReadObject.class));

        cacheCfg.setTypeMetadata(metas);

        cfg.setCacheConfiguration(cacheCfg);

        ignite = Ignition.start(cfg);

        cache = ignite.cache(null);
    }

    /**
     * Create type metadata for class.
     *
     * @param cls Class.
     * @return Type metadata.
     */
    private static CacheTypeMetadata metaForClass(Class cls) {
        CacheTypeMetadata meta = new CacheTypeMetadata();

        meta.setKeyType(Integer.class);
        meta.setValueType(cls);
        meta.setAscendingFields(Collections.<String, Class<?>>singletonMap("val", Integer.class));

        return meta;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        G.stopAll(true);

        ignite = null;
        cache = null;
    }

    /**
     * Test plain type.
     *
     * @throws Exception If failed.
     */
    public void testPlain() throws Exception {
        check(EntityPlain.class);
    }

    /**
     * Test Serializable type.
     *
     * @throws Exception If failed.
     */
    public void testSerializable() throws Exception {
        check(EntitySerializable.class);
    }

    /**
     * Test Externalizable type.
     *
     * @throws Exception If failed.
     */
    public void testExternalizable() throws Exception {
        check(EntityExternalizable.class);
    }

    /**
     * Test Binarylizable type.
     *
     * @throws Exception If failed.
     */
    public void testBinarylizable() throws Exception {
        check(EntityBinarylizable.class);
    }

    /**
     * Test type with readObject/writeObject methods.
     *
     * @throws Exception If failed.
     */
    public void testWriteReadObject() throws Exception {
        check(EntityWriteReadObject.class);
    }

    /**
     * Internal check routine.
     *
     * @param cls Entity class.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private void check(Class cls) throws Exception {
        cache.put(1, createInstance(cls, 10));
        cache.put(2, createInstance(cls, 20));
        cache.put(3, createInstance(cls, 30));

        Iterator iter = cache.query(new SqlQuery(cls, "val=20")).iterator();

        assert iter.hasNext();

        Cache.Entry res = (Cache.Entry)iter.next();

        assertEquals(2, res.getKey());
        assertEquals(Integer.valueOf(20), U.field(res.getValue(), "val"));

        assert !iter.hasNext();

        iter = cache.query(
            new SqlFieldsQuery("SELECT p.val FROM " + cls.getSimpleName() + " p WHERE p.val=20")).iterator();

        assert iter.hasNext();

        List<Object> fieldsRes = (List<Object>)iter.next();

        assertEquals(20, fieldsRes.get(0));

        assert !iter.hasNext();
    }

    /**
     * Create object instance.
     *
     * @param cls Class.
     * @param val Value.
     * @return Instance.
     */
    private static Object createInstance(Class cls, int val) {
        if (cls.equals(EntityPlain.class))
            return new EntityPlain(val);
        else if (cls.equals(EntitySerializable.class))
            return new EntitySerializable(val);
        else if (cls.equals(EntityExternalizable.class))
            return new EntityExternalizable(val);
        else if (cls.equals(EntityBinarylizable.class))
            return new EntityBinarylizable(val);
        else
            return new EntityWriteReadObject(val);
    }

    /**
     * @return Whether reflective serializer should be used.
     */
    protected boolean useReflectiveSerializer() {
        return false;
    }

    /**
     * Plain entry.
     */
    private static class EntityPlain {
        /** Value. */
        public int val;

        /**
         * Default constructor.
         */
        public EntityPlain() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param val Value.
         */
        public EntityPlain(int val) {
            this.val = val;
        }
    }

    /**
     * Serializable entity.
     */
    private static class EntitySerializable implements Serializable {
        /** Value. */
        public int val;

        /**
         * Default constructor.
         */
        public EntitySerializable() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param val Value.
         */
        public EntitySerializable(int val) {
            this.val = val;
        }
    }

    /**
     * Serializable entity.
     */
    private static class EntityExternalizable implements Externalizable {
        /** Value. */
        public int val;

        /**
         * Default constructor.
         */
        public EntityExternalizable() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param val Value.
         */
        public EntityExternalizable(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(val);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            val = in.readInt();
        }
    }

    /**
     * Serializable entity.
     */
    private static class EntityBinarylizable implements Binarylizable {
        /** Value. */
        public int val;

        /**
         * Default constructor.
         */
        public EntityBinarylizable() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param val Value.
         */
        public EntityBinarylizable(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeInt("val", val);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            val = reader.readInt("val");
        }
    }

    /**
     * Serializable entity.
     */
    private static class EntityWriteReadObject implements Serializable {
        /** Value. */
        public int val;

        /**
         * Default constructor.
         */
        public EntityWriteReadObject() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param val Value.
         */
        public EntityWriteReadObject(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        private void writeObject(ObjectOutputStream s) throws IOException{
            s.writeInt(val);
        }

        /** {@inheritDoc} */
        private void readObject(ObjectInputStream s) throws IOException, ClassNotFoundException {
            val = s.readInt();
        }
    }
}
