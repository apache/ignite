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

package org.apache.ignite.internal.processors.cache.index;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Verifies correct indexes naming for Optimized Marshaller with enabled persistence case.
 *
 * See IGNITE-6915 for details.
 */

public class OptimizedMarshallerIndexNameTest extends AbstractIndexingCommonTest {
    /** Test name 1 */
    private static final String TEST_NAME1 = "Name1";

    /** Test name 2 */
    private static final String TEST_NAME2 = "Name2";

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName, IgniteTestResources rsrcs)
        throws Exception {

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName, rsrcs);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(300L * 1024 * 1024).setPersistenceEnabled(true))
            .setStoragePath(workSubdir() + "/db")
            .setWalArchivePath(workSubdir() + "/db/wal/archive")
            .setWalPath(workSubdir() + "/db/wal")
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setMarshaller(new OptimizedMarshaller());

        return cfg;
    }

    /**
     * Creates cache configuration with required indexed types.
     *
     * @param name The name of the cache
     */
    @SuppressWarnings("deprecation")
    protected static CacheConfiguration<Object, Object> cacheConfiguration(String name) {
        CacheConfiguration<Object, Object> cfg = new CacheConfiguration<>(name);

        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setNearConfiguration(new NearCacheConfiguration<>());
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setEvictionPolicy(null);

        cfg.setIndexedTypes(
            UUID.class, Person.class,
            UUID.class, FalsePerson.class);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), workSubdir(), true));

        startGrid(getTestIgniteInstanceName());
        grid().active(true);
    }

    /**
     * Verifies that BPlusTree are not erroneously shared between tables in the same cache
     * due to IGNITE-6915 bug.
     */
    @Test
    public void testOptimizedMarshallerIndex() {

        // Put objects of different types into the same cache
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration("PersonEn");

        IgniteCache<Object, Object> cache = grid().getOrCreateCache(ccfg);

        cache.put(UUID.randomUUID(), new Person(TEST_NAME1, 42));
        cache.put(UUID.randomUUID(), new FalsePerson(32, TEST_NAME2));

        // Run query against one particular type
        SqlFieldsQueryEx qry = new SqlFieldsQueryEx(
            "select * from " + QueryUtils.typeName(FalsePerson.class), true);

        // If fix for IGNITE-6915 doesn't work you should see exception like the one below in the log:
        //
        // org.h2.jdbc.JdbcSQLException: General error: "class org.apache.ignite.IgniteCheckedException:
        // Failed to invoke getter method [type=int, property=name,
        // obj=org.apache.ignite.internal.processors.cache.index.OptimizedMarshallerIndexNameTest$Person@...:
        // org.apache.ignite.internal.processors.cache.index.OptimizedMarshallerIndexNameTest$Person@...,
        // getter=public int org.apache.ignite.internal.processors.cache.index.OptimizedMarshallerIndexNameTest$FalsePerson.getName()]"

        List<List<?>> res = cache.query(qry).getAll();

        assertEquals(1, res.size());
    }

    /**
     * Returns subdirectory of the work directory to put persistence store.
     * For this test it's a class name.
     *
     * @return The name of subdirectory (the short name of the test class).
     */
    @NotNull private String workSubdir() {
        return getClass().getSimpleName();
    }

    /** Entity to query. */
    public static class Person implements Externalizable {

        /** Person name. */
        @QuerySqlField(index = true, inlineSize = 0)
        private String name;

        /** Person age. */
        @QuerySqlField(index = true, inlineSize = 0)
        private int age;

        /** Creates a unnamed newborn person. */
        public Person() {
        }

        /**
         * Creates a person.
         *
         * @param name Name
         * @param age Age
         */
        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        /**
         * Returns name of the person.
         * @return The name of the person.
         */
        public String getName() {
            return name;
        }

        /**
         * Returns age of the person.
         * @return Person's age.
         */
        public int getAge() {
            return age;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeUTF(name);
            out.writeInt(age);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException {
            name = in.readUTF();
            age = in.readInt();
        }
    }

    /**
     * The class that should not be met in the Person table queried
     * due to mixing of {@link BPlusTree}-s.
     *
     * Note that the types of name and age are swapped.
     */
    public static class FalsePerson implements Externalizable {

        /** Person numeric name in future digital age */
        @QuerySqlField(index = true, inlineSize = 0)
        private int name;

        /** Age is a string. Life's road could be twisted. */
        @QuerySqlField(index = true, inlineSize = 0)
        private String age;

        /** Creates an anonymous baby. */
        public FalsePerson() {
        }

        /**
         * Creates a person of new type.
         *
         * @param name Numeric name.
         * @param age Digital age.
         */
        public FalsePerson(int name, String age) {
            this.name = name;
            this.age = age;
        }

        /**
         * Says how should you call this person.
         *
         * @return that digital name of the person.
         */
        public int getName() {
            return name;
        }

        /**
         * Makes you informed about person's bio.
         *
         * @return age as a string.
         */
        public String getAge() {
            return age;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(name);
            out.writeUTF(age);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException {
            name = in.readInt();
            age = in.readUTF();
        }
    }
}
