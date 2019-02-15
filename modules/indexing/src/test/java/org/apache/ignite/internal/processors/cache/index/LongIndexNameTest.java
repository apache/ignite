/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.index;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 *
 */
public class LongIndexNameTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(new DataStorageConfiguration())
            .setCacheConfiguration(new <String, Person>CacheConfiguration("cache")
                .setQueryEntities(getIndexCfg())
                .setAffinity(new RendezvousAffinityFunction(false, 16)));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLongIndexNames() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            IgniteCache cache = insertSomeData(ignite);

            QueryCursor cursor1 = cache.query(new SqlFieldsQuery("SELECT * FROM Person where name like '%Name 0'"));
            QueryCursor cursor1Idx = cache.query(new SqlFieldsQuery("SELECT * FROM Person where name = 'Name 0'"));

            QueryCursor cursor2 = cache.query(new SqlFieldsQuery("SELECT * FROM Person where age like '%0'"));
            QueryCursor cursor2Idx = cache.query(new SqlFieldsQuery("SELECT * FROM Person where age = 0"));

            assertEquals(cursor1.getAll().size(), cursor1Idx.getAll().size());
            assertEquals(cursor2.getAll().size(), cursor2Idx.getAll().size());

            ignite.close();

            Thread.sleep(2_000);

            ignite = startGrid(0);

            cache = insertSomeData(ignite);

            cursor1 = cache.query(new SqlFieldsQuery("SELECT * FROM Person where name like '%Name 0'"));
            cursor1Idx = cache.query(new SqlFieldsQuery("SELECT * FROM Person where name = 'Name 0'"));

            cursor2 = cache.query(new SqlFieldsQuery("SELECT * FROM Person where age like '%0'"));
            cursor2Idx = cache.query(new SqlFieldsQuery("SELECT * FROM Person where age = 0"));

            assertEquals(cursor1.getAll().size(), cursor1Idx.getAll().size());
            assertEquals(cursor2.getAll().size(), cursor2Idx.getAll().size());


        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    @NotNull private IgniteCache insertSomeData(Ignite ignite) {
        if (!ignite.active())
            ignite.active(true);

        IgniteCache<String, Person> cache = ignite.cache("cache");

        for (int i=0; i<10; i++)
            cache.put(String.valueOf(System.currentTimeMillis()), new Person("Name " + i, i));

        return cache;
    }

    /**
     *
     */
    public static List<QueryEntity> getIndexCfg() {
        ArrayList<QueryEntity> entities = new ArrayList<>();

        QueryEntity qe = new QueryEntity(String.class.getName(), Person.class.getName());

        LinkedHashMap<String, String> fieldsMap = new LinkedHashMap<>();
        fieldsMap.put("name", String.class.getName());
        fieldsMap.put("age", Integer.class.getName());

        qe.setFields(fieldsMap);

        ArrayList<QueryIndex> indices = new ArrayList<>();
        QueryIndex index = new QueryIndex("name", true, "LONG_NAME_123456789012345678901234567890" +
            "12345678901234567890123456789012345678901234567890123456789012345678901234567890");

        QueryIndex index2 = new QueryIndex("age", true, "AGE_IDX");
        indices.add(index);
        indices.add(index2);

        qe.setIndexes(indices);

        entities.add(qe);

        return entities;
    }

    /**
     *
     */
    private static class Person {
        /** */
        private String name;

        /** */
        private int age;

        /**
         *
         */
        public Person() {
            // No-op.
        }

        /**
         * @param name Name.
         * @param age Age.
         */
        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        /**
         * @return Name.
         */
        public String getName() {
            return name;
        }

        /**
         * @param name Name.
         */
        public void setName(String name) {
            this.name = name;
        }

        /**
         * @return Age.
         */
        public int getAge() {
            return age;
        }

        /**
         * @param age Age.
         */
        public void setAge(int age) {
            this.age = age;
        }
    }
}
