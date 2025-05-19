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

package org.apache.ignite.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryObjectEx;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.ClientProcessorMXBean;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Ignite {@link BinaryObject} API system tests.
 */
public class IgniteBinaryTest extends GridCommonAbstractTest {
    /**
     * Unmarshalling schema-less Ignite binary objects into Java static types.
     */
    @Test
    public void testUnmarshalSchemalessIgniteBinaries() throws Exception {
        int key = 1;
        Person val = new Person(key, "Joe");

        try (Ignite srv = Ignition.start(Config.getServerConfiguration())) {
            // Add an entry directly to the Ignite server. This stores a schema-less object in the cache and
            // does not register schema in the client's metadata cache.
            srv.cache(Config.DEFAULT_CACHE_NAME).put(key, val);

            try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))) {
                ClientCache<Integer, Person> cache = client.cache(Config.DEFAULT_CACHE_NAME);

                Person cachedVal = cache.get(key);

                assertEquals(val, cachedVal);
            }
        }
    }

    /**
     * Reading schema-less Ignite Binary object.
     */
    @Test
    public void testReadingSchemalessIgniteBinaries() throws Exception {
        int key = 1;
        Person val = new Person(key, "Joe");

        try (Ignite srv = Ignition.start(Config.getServerConfiguration())) {
            // Add an entry directly to the Ignite server. This stores a schema-less object in the cache and
            // does not register schema in the client's metadata cache.
            srv.cache(Config.DEFAULT_CACHE_NAME).put(key, val);

            try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))) {
                ClientCache<Integer, BinaryObject> cache = client.cache(Config.DEFAULT_CACHE_NAME).withKeepBinary();

                BinaryObject cachedVal = cache.get(key);

                assertEquals(val.getId(), cachedVal.field("id"));
                assertEquals(val.getName(), cachedVal.field("name"));
            }
        }
    }

    /**
     * Put/get operations with Ignite Binary Object API
     */
    @Test
    public void testBinaryObjectPutGet() throws Exception {
        int key = 1;

        try (Ignite ignored = Ignition.start(Config.getServerConfiguration())) {
            try (IgniteClient client =
                     Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))
            ) {
                IgniteBinary binary = client.binary();

                BinaryObject val = binary.builder("Person")
                    .setField("id", 1, int.class)
                    .setField("name", "Joe", String.class)
                    .build();

                ClientCache<Integer, BinaryObject> cache = client.cache(Config.DEFAULT_CACHE_NAME).withKeepBinary();

                cache.put(key, val);

                BinaryObject cachedVal =
                    client.cache(Config.DEFAULT_CACHE_NAME).<Integer, BinaryObject>withKeepBinary().get(key);

                assertBinaryObjectsEqual(val, cachedVal);
            }
        }
    }

    /**
     * Tests that {@code org.apache.ignite.cache.CacheInterceptor#onBeforePut(javax.cache.Cache.Entry, java.lang.Object)}
     * throws correct exception in case while cache operations are called from thin client. Only BinaryObject`s are
     * acceptable in this case.
     */
    @Test
    public void testBinaryWithNotGenericInterceptor() throws Exception {
        IgniteConfiguration ccfg = Config.getServerConfiguration()
            .setCacheConfiguration(new CacheConfiguration("test").setInterceptor(new ThinBinaryValueInterceptor()));

        String castErr = "cannot be cast to";
        String treeErr = "B+Tree is corrupted";

        ListeningTestLogger srvLog = new ListeningTestLogger(log);

        LogListener lsnrCast = LogListener.matches(castErr).
            andMatches(str -> !str.contains(treeErr)).build();

        srvLog.registerListener(lsnrCast);

        ccfg.setGridLogger(srvLog);

        try (Ignite ign = Ignition.start(ccfg)) {
            try (IgniteClient client =
                     Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))
            ) {
                ClientCache<Integer, ThinBinaryValue> cache = client.cache("test");

                try {
                    cache.put(1, new ThinBinaryValue());

                    fail();
                }
                catch (Exception e) {
                    assertFalse(X.getFullStackTrace(e).contains(castErr));
                }

                ClientProcessorMXBean serverMxBean =
                    getMxBean(ign.name(), "Clients", ClientListenerProcessor.class, ClientProcessorMXBean.class);

                serverMxBean.showFullStackOnClientSide(true);

                try {
                    cache.put(1, new ThinBinaryValue());
                }
                catch (Exception e) {
                    assertTrue(X.getFullStackTrace(e).contains(castErr));
                }
            }
        }

        assertTrue(lsnrCast.check());
    }

    /**
     * Test interceptor implementation.
     */
    private static class ThinBinaryValueInterceptor extends CacheInterceptorAdapter<String, ThinBinaryValue> {
        /** {@inheritDoc} */
        @Override public ThinBinaryValue onBeforePut(Cache.Entry<String, ThinBinaryValue> entry, ThinBinaryValue newVal) {
            return super.onBeforePut(entry, newVal);
        }
    }

    /**
     * Test value class.
     */
    private static class ThinBinaryValue {
    }

    /**
     * Check that binary types are registered for nested types too.
     * With enabled "CompactFooter" binary type schema also should be passed to server.
     */
    @Test
    public void testCompactFooterNestedTypeRegistration() throws Exception {
        try (Ignite ignite = Ignition.start(Config.getServerConfiguration())) {
            try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER)
                .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true)))
            ) {
                IgniteCache<Integer, Person[]> igniteCache = ignite.getOrCreateCache(Config.DEFAULT_CACHE_NAME);
                ClientCache<Integer, Person[]> clientCache = client.getOrCreateCache(Config.DEFAULT_CACHE_NAME);

                Integer key = 1;
                Person[] val = new Person[] {new Person(1, "Joe")};

                // Binary types should be registered for both "Person[]" and "Person" classes after this call.
                clientCache.put(key, val);

                // Check that we can deserialize on server using registered binary types.
                assertArrayEquals(val, igniteCache.get(key));
            }
        }
    }

    /**
     * Check that binary type schema updates are propagated from client to server and from server to client.
     */
    @Test
    public void testCompactFooterModifiedSchemaRegistration() throws Exception {
        try (Ignite ignite = Ignition.start(Config.getServerConfiguration())) {
            ignite.getOrCreateCache(Config.DEFAULT_CACHE_NAME);

            ClientConfiguration cfg = new ClientConfiguration().setAddresses(Config.SERVER)
                .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true));

            try (IgniteClient client1 = Ignition.startClient(cfg); IgniteClient client2 = Ignition.startClient(cfg)) {
                ClientCache<Integer, Object> cache1 = client1.cache(Config.DEFAULT_CACHE_NAME).withKeepBinary();
                ClientCache<Integer, Object> cache2 = client2.cache(Config.DEFAULT_CACHE_NAME).withKeepBinary();

                String type = "Person";

                // Register type and schema.
                BinaryObjectBuilder builder = client1.binary().builder(type);

                BinaryObject val1 = builder.setField("Name", "Person 1").build();

                cache1.put(1, val1);

                assertEquals("Person 1", ((BinaryObject)cache2.get(1)).field("Name"));

                // Update schema.
                BinaryObject val2 = builder.setField("Name", "Person 2").setField("Age", 2).build();

                cache1.put(2, val2);

                assertEquals("Person 2", ((BinaryObject)cache2.get(2)).field("Name"));
                assertEquals((Integer)2, ((BinaryObject)cache2.get(2)).field("Age"));
            }
        }
    }

    /**
     * Test custom binary type serializer.
     */
    @Test
    public void testBinarySerializer() throws Exception {
        BinarySerializer binSer = new BinarySerializer() {
            @Override public void writeBinary(Object obj, BinaryWriter writer) throws BinaryObjectException {
                writer.writeInt("f1", ((Person)obj).getId());
            }

            @Override public void readBinary(Object obj, BinaryReader reader) throws BinaryObjectException {
                ((Person)obj).setId(reader.readInt("f1"));
            }
        };

        BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration(Person.class.getName()).setSerializer(binSer);
        BinaryConfiguration binCfg = new BinaryConfiguration().setTypeConfigurations(Collections.singleton(typeCfg));

        try (Ignite ignite = Ignition.start(Config.getServerConfiguration().setBinaryConfiguration(binCfg))) {
            try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER)
                .setBinaryConfiguration(binCfg))) {
                IgniteCache<Integer, Person> igniteCache = ignite.getOrCreateCache(Config.DEFAULT_CACHE_NAME);
                ClientCache<Integer, Person> clientCache = client.getOrCreateCache(Config.DEFAULT_CACHE_NAME);

                Person val = new Person(123, "Joe");

                clientCache.put(1, val);

                assertEquals(val.getId(), clientCache.get(1).getId());
                assertNull(clientCache.get(1).getName());
                assertEquals(val.getId(), igniteCache.get(1).getId());
                assertNull(igniteCache.get(1).getName());
            }
        }
    }

    /**
     * Test custom binary type ID mapper.
     */
    @Test
    public void testBinaryIdMapper() throws Exception {
        BinaryIdMapper idMapper = new BinaryIdMapper() {
            @Override public int typeId(String typeName) {
                return typeName.hashCode() % 1000 + 1000;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return fieldName.hashCode();
            }
        };

        BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration(Person.class.getName()).setIdMapper(idMapper);
        BinaryConfiguration binCfg = new BinaryConfiguration().setTypeConfigurations(Collections.singleton(typeCfg));

        try (Ignite ignite = Ignition.start(Config.getServerConfiguration().setBinaryConfiguration(binCfg))) {
            try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER)
                .setBinaryConfiguration(binCfg))) {
                IgniteCache<Integer, Person> igniteCache = ignite.getOrCreateCache(Config.DEFAULT_CACHE_NAME);
                ClientCache<Integer, Person> clientCache = client.getOrCreateCache(Config.DEFAULT_CACHE_NAME);

                Person val = new Person(123, "Joe");

                clientCache.put(1, val);

                assertEquals(val, clientCache.get(1));
                assertEquals(val, igniteCache.get(1));
            }
        }
    }

    /**
     * Binary Object API:
     * {@link IgniteBinary#typeId(String)}
     * {@link IgniteBinary#toBinary(Object)}
     * {@link IgniteBinary#type(int)}
     * {@link IgniteBinary#type(Class)}
     * {@link IgniteBinary#type(String)}
     * {@link IgniteBinary#types()}
     * {@link IgniteBinary#buildEnum(String, int)}
     * {@link IgniteBinary#buildEnum(String, String)}
     * {@link IgniteBinary#registerEnum(String, Map)}
     */
    @Test
    public void testBinaryObjectApi() throws Exception {
        try (Ignite srv = Ignition.start(Config.getServerConfiguration())) {
            try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))) {
                // Use "server-side" IgniteBinary as a reference to test the thin client IgniteBinary against
                IgniteBinary refBinary = srv.binary();

                IgniteBinary binary = client.binary();

                Person obj = new Person(1, "Joe");

                int refTypeId = refBinary.typeId(Person.class.getName());
                int typeId = binary.typeId(Person.class.getName());

                assertEquals(refTypeId, typeId);

                BinaryObject refBinObj = refBinary.toBinary(obj);
                BinaryObject binObj = binary.toBinary(obj);

                assertBinaryObjectsEqual(refBinObj, binObj);

                assertBinaryTypesEqual(refBinary.type(typeId), binary.type(typeId));

                assertBinaryTypesEqual(refBinary.type(Person.class), binary.type(Person.class));

                assertBinaryTypesEqual(refBinary.type(Person.class.getName()), binary.type(Person.class.getName()));

                Collection<BinaryType> refTypes = refBinary.types();
                Collection<BinaryType> types = binary.types();

                assertEquals(refTypes.size(), types.size());

                BinaryObject refEnm = refBinary.buildEnum(Enum.class.getName(), Enum.DEFAULT.ordinal());
                BinaryObject enm = binary.buildEnum(Enum.class.getName(), Enum.DEFAULT.ordinal());

                assertBinaryObjectsEqual(refEnm, enm);

                Map<String, Integer> enumMap = Arrays.stream(Enum.values())
                    .collect(Collectors.toMap(java.lang.Enum::name, java.lang.Enum::ordinal));

                BinaryType refEnumType = refBinary.registerEnum(Enum.class.getName(), enumMap);
                BinaryType enumType = binary.registerEnum(Enum.class.getName(), enumMap);

                assertBinaryTypesEqual(refEnumType, enumType);

                refEnm = refBinary.buildEnum(Enum.class.getName(), Enum.DEFAULT.name());
                enm = binary.buildEnum(Enum.class.getName(), Enum.DEFAULT.name());

                assertBinaryObjectsEqual(refEnm, enm);
            }
        }
    }

    /**
     * The purpose of this test is to check that message which begins with the same byte as marshaller header can
     * be correctly unmarshalled.
     */
    @Test
    public void testBinaryTypeWithIdOfMarshallerHeader() throws Exception {
        try (Ignite ignite = Ignition.start(Config.getServerConfiguration())) {
            try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))) {
                int typeId = GridBinaryMarshaller.OBJ;

                BinaryObjectEx binObj = (BinaryObjectEx)ignite.binary().builder(Character.toString((char)typeId))
                        .setField("dummy", "dummy")
                        .build();

                assertEquals(typeId, binObj.typeId());

                BinaryType type = client.binary().type(typeId);

                assertEquals(binObj.type().typeName(), type.typeName());
            }
        }
    }

    /** */
    @Test
    public void testBinaryMetaSendAfterServerRestart() {
        String name = "name";

        List<Function<String, Object>> factories = new ArrayList<>();
        factories.add(n -> new Person(0, n));
        factories.add(PersonBinarylizable::new);

        for (Function<String, Object> factory : factories) {
            Ignite ignite = null;
            IgniteClient client = null;

            try {
                ignite = Ignition.start(Config.getServerConfiguration());
                client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER));

                ClientCache<Integer, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

                Object person = factory.apply(name);

                log.info(">>>> Check object class: " + person.getClass().getSimpleName());

                cache.put(0, person);

                ignite.close();

                ignite = Ignition.start(Config.getServerConfiguration());

                cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

                cache.put(0, person);

                // Perform any action on server-side with binary object to ensure binary meta exists on node.
                assertEquals(name, cache.invoke(0, new ExtractNameEntryProcessor()));
            }
            finally {
                U.close(client, log);
                U.close(ignite, log);
            }
        }
    }

    /** */
    private void assertBinaryTypesEqual(BinaryType exp, BinaryType actual) {
        assertEquals(exp.typeId(), actual.typeId());
        assertEquals(exp.typeName(), actual.typeName());
        assertArrayEquals(exp.fieldNames().toArray(), actual.fieldNames().toArray());

        for (String f : exp.fieldNames())
            assertEquals(exp.fieldTypeName(f), actual.fieldTypeName(f));

        assertEquals(exp.affinityKeyFieldName(), actual.affinityKeyFieldName());
        assertEquals(exp.isEnum(), actual.isEnum());
    }

    /** */
    private void assertBinaryObjectsEqual(BinaryObject exp, BinaryObject actual) throws Exception {
        assertBinaryTypesEqual(exp.type(), actual.type());

        for (String f : exp.type().fieldNames()) {
            Object expVal = exp.field(f);

            Class<?> cls = expVal.getClass();

            if (cls.getMethod("equals", Object.class).getDeclaringClass() == cls)
                assertEquals(expVal, actual.field(f));
        }

        if (exp.type().isEnum())
            assertEquals(exp.enumOrdinal(), actual.enumOrdinal());
    }

    /**
     * Enumeration for tests.
     */
    private enum Enum {
        /** Default. */
        DEFAULT
    }

    /** */
    private static class ExtractNameEntryProcessor implements EntryProcessor<Integer, Object, String> {
        /** {@inheritDoc} */
        @Override public String process(MutableEntry<Integer, Object> entry, Object... arguments) {
            return ((BinaryObject)entry.getValue()).field("name").toString();
        }
    }
}
