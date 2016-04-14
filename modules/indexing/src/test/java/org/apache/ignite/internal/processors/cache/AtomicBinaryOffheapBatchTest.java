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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The test are checking batch operation onto atomic offheap cache with per certain key and value types.
 */
public class AtomicBinaryOffheapBatchTest extends IgniteCacheAbstractTest {

    /**
     * Size of batch in operation
     */
    public static final int BATCH_SIZE = 500;

    @Override
    protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new BinaryMarshaller());
        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    @Override
    protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setMemoryMode(memoryMode());

        cfg.setIndexedTypes(Integer.class, Person.class, Integer.class, Organization.class);

        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override
    protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override
    protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override
    protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.ATOMIC;
    }

    /** {@inheritDoc} */
    @Override
    protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /**
     * @return Cache memory mode.
     */
    protected CacheMemoryMode memoryMode() {
        return CacheMemoryMode.OFFHEAP_TIERED;
    }

    /** {@inheritDoc} */
    @Override
    protected CacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return CacheAtomicWriteOrderMode.PRIMARY;
    }

    /**
     * Test method.
     *
     * @throws Exception If fail.
     */
    public void testBatchOperations() throws Exception {
        try (IgniteCache defaultCache = ignite(0).cache(null)) {
            loadingCacheAnyDate();

            for (int cnt = 0; cnt < 200; cnt++) {
                Map<Integer, Person> putMap1 = new TreeMap<>();
                for (int i = 0; i < BATCH_SIZE; i++)
                    putMap1.put(i, new Person(i, i + 1, String.valueOf(i), String.valueOf(i + 1), i / 0.99));

                defaultCache.putAll(putMap1);

                Map<Integer, Organization> putMap2 = new TreeMap<>();
                for (int i = BATCH_SIZE / 2; i < BATCH_SIZE * 3 / 2; i++)
                    putMap2.put(i, new Organization(i, String.valueOf(i)));

                defaultCache.putAll(putMap2);

                Set<Integer> keySet = new TreeSet<>();

                for (int i = 0; i < BATCH_SIZE * 3 / 2; i += 2)
                    keySet.add(i);

                defaultCache.removeAll(keySet);

                keySet = new TreeSet<>();

                for (int i = 1; i < BATCH_SIZE * 3 / 2; i += 2)
                    keySet.add(i);

                defaultCache.invokeAll(keySet, new EntryProcessor() {
                    @Override
                    public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
                        Object value = entry.getValue();
                        entry.remove();
                        return value;
                    }
                });
            }
        }
    }

    /**
     * Loading date into cache
     */
    private void loadingCacheAnyDate() {
        try (IgniteDataStreamer streamer = ignite(0).dataStreamer(null)) {
            for (int i = 0; i < 30_000; i++) {
                if (i % 2 == 0)
                    streamer.addData(i, new Person(i, i + 1, String.valueOf(i), String.valueOf(i + 1), i / 0.99));
                else
                    streamer.addData(i, new Organization(i, String.valueOf(i)));
            }
        }
    }

    /**
     * Ignite cache value class.
     */
    private static class Person implements Binarylizable {

        /** Person ID. */
        @QuerySqlField(index = true)
        private int id;

        /** Organization ID. */
        @QuerySqlField(index = true)
        private int orgId;

        /** First name (not-indexed). */
        @QuerySqlField
        private String firstName;

        /** Last name (not indexed). */
        @QuerySqlField
        private String lastName;

        /** Salary. */
        @QuerySqlField(index = true)
        private double salary;

        /**
         * Constructs empty person.
         */
        public Person() {
            // No-op.
        }

        /**
         * Constructs person record.
         *
         * @param id Person ID.
         * @param orgId Organization ID.
         * @param firstName First name.
         * @param lastName Last name.
         * @param salary Salary.
         */
        public Person(int id, int orgId, String firstName, String lastName, double salary) {
            this.id = id;
            this.orgId = orgId;
            this.firstName = firstName;
            this.lastName = lastName;
            this.salary = salary;
        }

        /**
         * @return Person id.
         */
        public int getId() {
            return id;
        }

        /**
         * @param id Person id.
         */
        public void setId(int id) {
            this.id = id;
        }

        /**
         * @return Organization id.
         */
        public int getOrganizationId() {
            return orgId;
        }

        /**
         * @param orgId Organization id.
         */
        public void setOrganizationId(int orgId) {
            this.orgId = orgId;
        }

        /**
         * @return Person first name.
         */
        public String getFirstName() {
            return firstName;
        }

        /**
         * @param firstName Person first name.
         */
        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        /**
         * @return Person last name.
         */
        public String getLastName() {
            return lastName;
        }

        /**
         * @param lastName Person last name.
         */
        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        /**
         * @return Salary.
         */
        public double getSalary() {
            return salary;
        }

        /**
         * @param salary Salary.
         */
        public void setSalary(double salary) {
            this.salary = salary;
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeInt("id", id);
            writer.writeInt("orgId", orgId);
            writer.writeString("firstName", firstName);
            writer.writeString("lastName", lastName);
            writer.writeDouble("salary", salary);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            id = reader.readInt("id");
            orgId = reader.readInt("orgId");
            firstName = reader.readString("firstName");
            lastName = reader.readString("lastName");
            salary = reader.readDouble("salary");
        }
    }

    /**
     * Ignite cache value class with indexed field.
     */
    private static class Organization implements Binarylizable {

        /** Organization ID. */
        @QuerySqlField(index = true)
        private int id;

        /** Organization name. */
        @QuerySqlField(index = true)
        private String name;

        /**
         * Constructs empty organization.
         */
        public Organization() {
            // No-op.
        }

        /**
         * Constructs organization with given ID.
         *
         * @param id Organization ID.
         * @param name Organization name.
         */
        public Organization(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /**
         * @return Organization id.
         */
        public int getId() {
            return id;
        }

        /**
         * @param id Organization id.
         */
        public void setId(int id) {
            this.id = id;
        }

        /**
         * @return Organization name.
         */
        public String getName() {
            return name;
        }

        /**
         * @param name Organization name.
         */
        public void setName(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeInt("id", id);
            writer.writeString("name", name);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            id = reader.readInt("id");
            name = reader.readString("name");
        }
    }
}
