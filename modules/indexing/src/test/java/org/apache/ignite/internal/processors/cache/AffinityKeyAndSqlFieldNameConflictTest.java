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
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * IGNITE-7793 SQL does not work if value has sql field which name equals to affinity key name
 */
public class AffinityKeyAndSqlFieldNameConflictTest extends GridCommonAbstractTest {
    /** */
    private static final String PERSON_CACHE = "person";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheKeyConfiguration keyCfg = new CacheKeyConfiguration(PersonKey.class);

        cfg.setCacheKeyConfiguration(keyCfg);

        CacheConfiguration ccfg = new CacheConfiguration(PERSON_CACHE);
        ccfg.setIndexedTypes(PersonKey.class, Person.class);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testNameConflict() throws Exception {
        startGrid(2);

        Ignite g = grid(2);

        IgniteCache<Object, Object> personCache = g.cache(PERSON_CACHE);

        personCache.put(new PersonKey(1, "o1"), new Person("p1"));

        SqlFieldsQuery query = new SqlFieldsQuery("select * from \"" + PERSON_CACHE + "\"." + Person.class.getSimpleName() + " it where it.name=?");

        List<List<?>> result = personCache.query(query.setArgs("p1")).getAll();

        assertEquals(result.size(), 1);
    }

    /**
     *
     */
    public static class PersonKey {
        /** */
        @QuerySqlField
        private int id;

        /** */
        @AffinityKeyMapped
        private String name;

        /**
         * @param id Key.
         * @param name Affinity key.
         */
        public PersonKey(int id, String name) {
            this.id = id;
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            PersonKey other = (PersonKey)o;

            return id == other.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    private static class Person implements Serializable {

        /** */
        @QuerySqlField
        String name;

        /**
         * @param name name.
         */
        public Person(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }
    }
}