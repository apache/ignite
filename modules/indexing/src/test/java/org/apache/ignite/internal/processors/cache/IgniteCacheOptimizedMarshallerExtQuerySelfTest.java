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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.marshaller.optimized.*;

import javax.cache.*;
import java.io.*;
import java.util.*;

import static org.apache.ignite.cache.CacheMode.*;

/**
 * Tests queries with {@link OptimizedMarshaller} enabled.
 */
public class IgniteCacheOptimizedMarshallerExtQuerySelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setTypeMetadata(cacheTypeMetadata());

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setNearConfiguration(null);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        IgniteCache<Object, Object> cache = grid(0).cache(null);

        for (int i = 0; i < 50; i++) {
            Address addr = new Address();

            addr.street = "Street " + i;
            addr.zip = i;

            Person p = new Person();

            p.name = "Person " + i;
            p.salary = (i + 1) * 100;
            p.address = addr;

            Customer customer = new Customer();
            customer.id = i;
            customer.company = "Company " + addr.street;
            customer.info = p;

            cache.put(i, p);
            cache.put(i + 200, customer);
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testSimpleQuery() throws Exception {
        IgniteCache<Integer, Person> cache = grid(0).cache(null);

        Collection<Cache.Entry<Integer, Person>> entries = cache.query(new SqlQuery<Integer, Person>(
            "Person", "name is not null")).getAll();

        assertEquals(50, entries.size());

        for (Cache.Entry<Integer, Person> entry : entries) {
            int id = entry.getKey();
            Person p = entry.getValue();

            assertEquals("Person " + id, p.name);
            assertEquals((id + 1) * 100, p.salary);
            assertEquals("Street " + id, p.address.street);
            assertEquals(id, p.address.zip);
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testNestedFieldsQuery() throws Exception {
        IgniteCache<Integer, Person> cache = grid(0).cache(null);

        Collection<Cache.Entry<Integer, Person>> entries = cache.query(new SqlQuery<Integer, Person>(
            "Person", "name is not null AND (zip = 1 OR zip = 2)")).getAll();

        assertEquals(2, entries.size());

        for (Cache.Entry<Integer, Person> entry : entries) {
            int id = entry.getKey();
            Person p = entry.getValue();

            assertEquals("Person " + id, p.name);
            assertEquals((id + 1) * 100, p.salary);
            assertEquals("Street " + id, p.address.street);
            assertEquals(id, p.address.zip);
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testFieldsQuery() throws Exception {
        IgniteCache<Integer, Person> cache = grid(0).cache(null);

        QueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery("select name, address, zip" +
                                                                      " from Person where zip IN (1,2)"));

        List<?> result = cur.getAll();

        assertTrue(result.size() == 2);

        for (Object row : result) {
            ArrayList<Object> list = (ArrayList<Object>)row;

            Address addr = (Address)list.get(1);
            int zip = (int)list.get(2);

            assertEquals(addr.zip, zip);
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testSimpleMarshalAwareQuery() throws Exception {
        IgniteCache<Integer, Customer> cache = grid(0).cache(null);

        Collection<Cache.Entry<Integer, Customer>> entries = cache.query(new SqlQuery<Integer, Customer>(
            "Customer", "info is not null")).getAll();

        assertEquals(50, entries.size());

        for (Cache.Entry<Integer, Customer> entry : entries) {
            int id = entry.getKey();
            Customer c = entry.getValue();

            id -= 200;

            assertEquals("Person " + id, c.info.name);
            assertEquals((id + 1) * 100, c.info.salary);
            assertEquals("Company Street " + id, c.company);
            assertEquals(id, c.id);
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testNestedFieldsMarshalAwareQuery() throws Exception {
        IgniteCache<Integer, Customer> cache = grid(0).cache(null);

        Collection<Cache.Entry<Integer, Customer>> entries = cache.query(new SqlQuery<Integer, Customer>(
            "Customer", "name is not null AND (zip = 1 OR zip = 2)")).getAll();

        assertEquals(2, entries.size());

        for (Cache.Entry<Integer, Customer> entry : entries) {
            int id = entry.getKey();
            Customer c = entry.getValue();

            id -= 200;

            assertEquals("Person " + id, c.info.name);
            assertEquals((id + 1) * 100, c.info.salary);
            assertEquals("Company Street " + id, c.company);
            assertEquals(id, c.info.address.zip);
        }
    }

    /**
     * @throws Exception In case of error.
     */
    public void testFieldsMarshalAwareQuery() throws Exception {
        IgniteCache<Integer, Customer> cache = grid(0).cache(null);

        QueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery("select name, company, zip" +
                                                                      " from Customer where zip IN (1,2)"));

        List<?> result = cur.getAll();

        assertTrue(result.size() == 2);

        for (Object row : result) {
            ArrayList<Object> list = (ArrayList<Object>)row;

            assertNotNull(list.get(0));
            assertNotNull(list.get(1));
            assert (int)list.get(2) > 0;
        }
    }

    /**
     * @return Cache type metadata.
     */
    private Collection<CacheTypeMetadata> cacheTypeMetadata() {
        CacheTypeMetadata personMeta = new CacheTypeMetadata();

        personMeta.setValueType(Person.class);

        Map<String, Class<?>> personQryFields = new HashMap<>();

        personQryFields.put("name", String.class);
        personQryFields.put("salary", Integer.class);
        personQryFields.put("address", Address.class);
        personQryFields.put("address.zip", Integer.class);

        personMeta.setQueryFields(personQryFields);


        CacheTypeMetadata addrMeta = new CacheTypeMetadata();

        addrMeta.setValueType(Address.class);

        Map<String, Class<?>> addrQryFields = new HashMap<>();

        addrQryFields.put("street", String.class);
        addrQryFields.put("zip", Integer.class);

        addrMeta.setQueryFields(addrQryFields);

        CacheTypeMetadata customerMeta = new CacheTypeMetadata();

        customerMeta.setValueType(Customer.class);

        Map<String, Class<?>> customerIndexFields = new HashMap<>();
        customerIndexFields.put("id", Integer.class);

        customerMeta.setAscendingFields(customerIndexFields);

        Map<String, Class<?>> customerQueryFields = new HashMap<>();
        customerQueryFields.put("info", Person.class);
        customerQueryFields.put("info.address.zip", Integer.class);
        customerQueryFields.put("info.name", String.class);
        customerQueryFields.put("company", String.class);

        customerMeta.setQueryFields(customerQueryFields);

        return Arrays.asList(personMeta, addrMeta, customerMeta);
    }

    /**
     *
     */
    private static class Person {
        /** */
        public String name;

        /** */
        public int salary;

        /** */
        public Address address;
    }

    /**
     *
     */
    private static class Address {
        /** */
        public String street;

        /** */
        public int zip;
    }

    /**
     *
     */
    private static class Customer implements OptimizedMarshalAware {
        /** */
        private int id;

        /** */
        private Person info;

        /** */
        private String company;

        public Customer() {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void writeFields(OptimizedFieldsWriter writer) throws IOException {
            writer.writeInt("id", id);
            writer.writeObject("info", info);
            writer.writeString("company", company);
        }

        /** {@inheritDoc} */
        @Override public void readFields(OptimizedFieldsReader reader) throws IOException {
            info = reader.readObject("info");
            company = reader.readString("company");
            id = reader.readInt("id");
        }
    }
}
