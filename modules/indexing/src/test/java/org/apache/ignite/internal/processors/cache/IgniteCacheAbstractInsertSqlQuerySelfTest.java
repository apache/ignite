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

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryAbstractIdentityResolver;
import org.apache.ignite.binary.BinaryArrayIdentityResolver;
import org.apache.ignite.binary.BinaryFieldIdentityResolver;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
@SuppressWarnings("unchecked")
public abstract class IgniteCacheAbstractInsertSqlQuerySelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    protected final Marshaller marsh;

    /**
     *
     */
    IgniteCacheAbstractInsertSqlQuerySelfTest() {
        try {
            marsh = IgniteTestResources.getMarshaller();
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * @return whether {@link #marsh} is an instance of {@link BinaryMarshaller} or not.
     */
    boolean isBinaryMarshaller() {
        return marsh instanceof BinaryMarshaller;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        BinaryConfiguration binCfg = new BinaryConfiguration();

        binCfg.setTypeConfigurations(Arrays.asList(
            new BinaryTypeConfiguration() {{
                setTypeName(Key.class.getName());

                setIdentityResolver(BinaryArrayIdentityResolver.instance());
            }},
            new BinaryTypeConfiguration() {{
                setTypeName(Key2.class.getName());

                setIdentityResolver(BinaryArrayIdentityResolver.instance());
            }},
            new BinaryTypeConfiguration() {{
                setTypeName(Key3.class.getName());

                setIdentityResolver(new BinaryFieldIdentityResolver().setFieldNames("key"));
            }},
            new BinaryTypeConfiguration() {{
                setTypeName(Key4.class.getName());

                setIdentityResolver(new Key4Id());
            }}
        ));

        cfg.setBinaryConfiguration(binCfg);

        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(3, false);

        if (!isBinaryMarshaller())
            createCaches();
        else
            createBinaryCaches();
    }

    /**
     *
     */
    protected void createCaches() {
        ignite(0).createCache(cacheConfig("S2P", true, false, String.class, Person.class, String.class, String.class));
        ignite(0).createCache(cacheConfig("I2P", true, false, Integer.class, Person.class));
        ignite(0).createCache(cacheConfig("K2P", true, false, Key.class, Person.class));
        ignite(0).createCache(cacheConfig("K22P", true, true, Key2.class, Person2.class));
        ignite(0).createCache(cacheConfig("I2I", true, false, Integer.class, Integer.class));
    }

    /**
     *
     */
    final void createBinaryCaches() {
        {
            CacheConfiguration s2pCcfg = cacheConfig("S2P", true, false);

            QueryEntity s2p = new QueryEntity(String.class.getName(), "Person");

            LinkedHashMap<String, String> flds = new LinkedHashMap<>();

            flds.put("id", Integer.class.getName());
            flds.put("firstName", String.class.getName());

            s2p.setFields(flds);

            s2p.setIndexes(Collections.<QueryIndex>emptyList());

            QueryEntity s2s = new QueryEntity(String.class.getName(), String.class.getName());

            s2pCcfg.setQueryEntities(Arrays.asList(s2p, s2s));

            ignite(0).createCache(s2pCcfg);
        }

        {
            CacheConfiguration i2pCcfg = cacheConfig("I2P", true, false);

            QueryEntity i2p = new QueryEntity(Integer.class.getName(), "Person");

            LinkedHashMap<String, String> flds = new LinkedHashMap<>();

            flds.put("id", Integer.class.getName());
            flds.put("firstName", String.class.getName());

            i2p.setFields(flds);

            i2p.setIndexes(Collections.<QueryIndex>emptyList());

            i2pCcfg.setQueryEntities(Collections.singletonList(i2p));

            ignite(0).createCache(i2pCcfg);
        }

        {
            CacheConfiguration k2pCcfg = cacheConfig("K2P", true, false);

            QueryEntity k2p = new QueryEntity(Key.class.getName(), "Person");

            k2p.setKeyFields(Collections.singleton("key"));

            LinkedHashMap<String, String> flds = new LinkedHashMap<>();

            flds.put("key", Integer.class.getName());
            flds.put("id", Integer.class.getName());
            flds.put("firstName", String.class.getName());

            k2p.setFields(flds);

            k2p.setIndexes(Collections.<QueryIndex>emptyList());

            k2pCcfg.setQueryEntities(Collections.singletonList(k2p));

            ignite(0).createCache(k2pCcfg);
        }

        {
            CacheConfiguration k22pCcfg = cacheConfig("K22P", true, true);

            QueryEntity k22p = new QueryEntity(Key2.class.getName(), "Person2");

            k22p.setKeyFields(Collections.singleton("Id"));

            LinkedHashMap<String, String> flds = new LinkedHashMap<>();

            flds.put("Id", Integer.class.getName());
            flds.put("id", Integer.class.getName());
            flds.put("firstName", String.class.getName());
            flds.put("IntVal", Integer.class.getName());

            k22p.setFields(flds);

            k22p.setIndexes(Collections.<QueryIndex>emptyList());

            k22pCcfg.setQueryEntities(Collections.singletonList(k22p));

            ignite(0).createCache(k22pCcfg);
        }

        {
            CacheConfiguration k32pCcfg = cacheConfig("K32P", true, false);

            QueryEntity k32p = new QueryEntity(Key3.class.getName(), "Person");

            k32p.setKeyFields(new HashSet<>(Arrays.asList("key", "strKey")));

            LinkedHashMap<String, String> flds = new LinkedHashMap<>();

            flds.put("key", Integer.class.getName());
            flds.put("strKey", String.class.getName());
            flds.put("id", Integer.class.getName());
            flds.put("firstName", String.class.getName());

            k32p.setFields(flds);

            k32p.setIndexes(Collections.<QueryIndex>emptyList());

            k32pCcfg.setQueryEntities(Collections.singletonList(k32p));

            ignite(0).createCache(k32pCcfg);
        }

        {
            CacheConfiguration k42pCcfg = cacheConfig("K42P", true, false);

            QueryEntity k42p = new QueryEntity(Key4.class.getName(), "Person");

            k42p.setKeyFields(new HashSet<>(Arrays.asList("key", "strKey")));

            LinkedHashMap<String, String> flds = new LinkedHashMap<>();

            flds.put("key", Integer.class.getName());
            flds.put("strKey", String.class.getName());
            flds.put("id", Integer.class.getName());
            flds.put("firstName", String.class.getName());

            k42p.setFields(flds);

            k42p.setIndexes(Collections.<QueryIndex>emptyList());

            k42pCcfg.setQueryEntities(Collections.singletonList(k42p));

            ignite(0).createCache(k42pCcfg);
        }

        {
            CacheConfiguration i2iCcfg = cacheConfig("I2I", true, false);

            QueryEntity i2i = new QueryEntity(Integer.class.getName(), Integer.class.getName());

            i2i.setFields(new LinkedHashMap<String, String>());

            i2i.setIndexes(Collections.<QueryIndex>emptyList());

            i2iCcfg.setQueryEntities(Collections.singletonList(i2i));

            ignite(0).createCache(i2iCcfg);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite(0).cache("S2P").clear();
        ignite(0).cache("I2P").clear();
        ignite(0).cache("K2P").clear();
        ignite(0).cache("K22P").clear();
        ignite(0).cache("I2I").clear();

        if (isBinaryMarshaller()) {
            ignite(0).cache("K32P").clear();
            ignite(0).cache("K42P").clear();
        }

        super.afterTest();
    }

    /**
     *
     */
    Object createPerson(int id, String name) {
        if (!isBinaryMarshaller()) {
            Person p = new Person(id);
            p.name = name;

            return p;
        }
        else {
            BinaryObjectBuilder o = grid(0).binary().builder("Person");
            o.setField("id", id);
            o.setField("name", name);

            return o.build();
        }
    }

    /**
     *
     */
    Object createPerson2(int id, String name, int valFld) {
        if (!isBinaryMarshaller()) {
            Person2 p = new Person2(id);
            p.name = name;
            p.IntVal = valFld;

            return p;
        }
        else {
            BinaryObjectBuilder o = grid(0).binary().builder("Person2");
            o.setField("id", id);
            o.setField("name", name);
            o.setField("IntVal", valFld);

            return o.build();
        }
    }

    /**
     * @param name Cache name.
     * @param partitioned Partition or replicated cache.
     * @param escapeSql whether identifiers should be quoted - see {@link CacheConfiguration#setSqlEscapeAll}
     * @param idxTypes Indexed types.
     * @return Cache configuration.
     */
    private static CacheConfiguration cacheConfig(String name, boolean partitioned, boolean escapeSql, Class<?>... idxTypes) {
        return new CacheConfiguration()
            .setName(name)
            .setCacheMode(partitioned ? CacheMode.PARTITIONED : CacheMode.REPLICATED)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setBackups(1)
            .setSqlEscapeAll(escapeSql)
            .setIndexedTypes(idxTypes);
    }

    /**
     *
     */
    protected final static class Key implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        public Key(int key) {
            this.key = key;
        }

        /** */
        @QuerySqlField
        public final int key;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key key1 = (Key) o;

            return key == key1.key;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key;
        }
    }

    /**
     *
     */
    protected final static class Key2 implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        public Key2(int Id) {
            this.Id = Id;
        }

        /** */
        @QuerySqlField
        public final int Id;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key2 key1 = (Key2) o;

            return Id == key1.Id;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Id;
        }
    }

    /**
     *
     */
    final static class Key3 implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        public Key3(int key) {
            this.key = key;
            this.strKey = Integer.toString(key);
        }

        /** */
        @QuerySqlField
        public final int key;

        /** */
        @QuerySqlField
        public final String strKey;
    }

    /**
     *
     */
    final static class Key4 implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        public Key4(int key) {
            this.key = key;
            this.strKey = Integer.toString(key);
        }

        /** */
        @QuerySqlField
        public final int key;

        /** */
        @QuerySqlField
        public final String strKey;
    }

    /**
     *
     */
    final static class Key4Id extends BinaryAbstractIdentityResolver {
        /** {@inheritDoc} */
        @Override protected int hashCode0(BinaryObject obj) {
            return (int) obj.field("key") * 100;
        }

        /** {@inheritDoc} */
        @Override protected boolean equals0(BinaryObject o1, BinaryObject o2) {
            return (int) o1.field("key") == (int) o2.field("key");
        }
    }

    /**
     *
     */
    protected static class Person implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @SuppressWarnings("unused")
        private Person() {
            // No-op.
        }

        /** */
        public Person(int id) {
            this.id = id;
        }

        /** */
        @QuerySqlField
        protected int id;

        /** */
        @QuerySqlField(name = "firstName")
        protected String name;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Person person = (Person) o;

            if (id != person.id) return false;
            return name != null ? name.equals(person.name) : person.name == null;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = id;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            return result;
        }
    }

    /**
     *
     */
    protected static class Person2 extends Person {
        /** */
        @SuppressWarnings("unused")
        private Person2() {
            // No-op.
        }

        /** */
        public Person2(int id) {
            super(id);
        }

        /** */
        @QuerySqlField
        public int IntVal;
    }
}
