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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Ignite {@link BinaryObject} API system tests.
 */
public class IgniteBinaryTest {
    /** Per test timeout */
    @Rule
    public Timeout globalTimeout = new Timeout((int) GridTestUtils.DFLT_TEST_TIMEOUT);

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

                BinaryObjectImpl binObj = (BinaryObjectImpl)ignite.binary().builder(Character.toString((char)typeId))
                        .setField("dummy", "dummy")
                        .build();

                assertEquals(typeId, binObj.typeId());

                BinaryType type = client.binary().type(typeId);

                assertEquals(binObj.type().typeName(), type.typeName());
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
        /** Default. */DEFAULT
    }
}
