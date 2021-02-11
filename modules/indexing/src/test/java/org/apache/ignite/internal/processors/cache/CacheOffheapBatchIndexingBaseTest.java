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

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests various cache operations with indexing enabled.
 */
public abstract class CacheOffheapBatchIndexingBaseTest extends GridCommonAbstractTest {
    /**
     * Load data into cache
     *
     * @param name Cache name.
     */
    protected void preload(String name) {
        try (IgniteDataStreamer<Object, Object> streamer = ignite(0).dataStreamer(name)) {
            for (int i = 0; i < 30_000; i++) {
                if (i % 2 == 0)
                    streamer.addData(i, new Person(i, i + 1, String.valueOf(i), String.valueOf(i + 1), salary(i)));
                else
                    streamer.addData(i, new Organization(i, String.valueOf(i)));
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(0);
    }

    /**
     * @param base Base.
     * @return Salary.
     */
    protected double salary(int base) {
        return base * 100.;
    }

    /**
     * @param indexedTypes indexed types for cache.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Object, Object> cacheConfiguration(Class<?>[] indexedTypes) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setIndexedTypes(indexedTypes);

        return ccfg;
    }

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
