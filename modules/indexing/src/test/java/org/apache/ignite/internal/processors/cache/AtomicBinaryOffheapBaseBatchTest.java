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

/**
 * The test are checking batch operation onto atomic offheap cache with per certain key and value types.
 */
public abstract class AtomicBinaryOffheapBaseBatchTest extends IgniteCacheAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setMemoryMode(memoryMode());

        int size = onHeapRowCacheSize();

        if (size > 0 )
            cfg.setSqlOnheapRowCacheSize(size);

        cfg.setIndexedTypes(indexedTypes());

        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC);

        return cfg;
    }

    /**
     * Row off-heap cache. Zero is used for default value (10_000).
     * @return row heap cache size in bytes.
     */
    protected int onHeapRowCacheSize() {
        return 0;
    }

    /**
     * Indexed types for test.
     * @return array of classes for indexing.
     */
    protected Class<?>[] indexedTypes() {
        return new Class<?>[]{Integer.class, Person.class, Integer.class, Organization.class};
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return CacheAtomicityMode.ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected NearCacheConfiguration nearConfiguration() {
        return null;
    }

    /**
     * @return Cache memory mode.
     */
    protected CacheMemoryMode memoryMode() {
        return CacheMemoryMode.OFFHEAP_TIERED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return CacheAtomicWriteOrderMode.PRIMARY;
    }

    /**
     * Test method.
     *
     * @throws Exception If fail.
     */
    public abstract void testBatchOperations() throws Exception;

    /**
     * Ignite cache value class.
     */
    protected static class Person implements Binarylizable {

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
    protected static class Organization implements Binarylizable {

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
