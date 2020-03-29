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

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.junit.Test;

/**
 *
 */
@SuppressWarnings("unchecked")
public class IgniteCacheDeleteSqlQuerySelfTest extends IgniteCacheAbstractSqlDmlQuerySelfTest {
    /**
     *
     */
    @Test
    public void testDeleteSimple() {
        IgniteCache p = cache();

        QueryCursor<List<?>> c = p.query(new SqlFieldsQuery("delete from Person p where length(p._key) = 2 " +
            "or p.secondName like '%ite'"));

        c.iterator();

        c = p.query(new SqlFieldsQuery("select _key, _val, * from Person order by id"));

        List<List<?>> leftovers = c.getAll();

        assertEquals(2, leftovers.size());

        assertEqualsCollections(Arrays.asList("SecondKey", createPerson(2, "Joe", "Black"), 2, "Joe", "Black"),
            leftovers.get(0));

        assertEqualsCollections(Arrays.asList("f0u4thk3y", createPerson(4, "Jane", "Silver"), 4, "Jane", "Silver"),
            leftovers.get(1));
    }

    /**
     *
     */
    @Test
    public void testDeleteSingle() {
        IgniteCache p = cache();

        QueryCursor<List<?>> c = p.query(new SqlFieldsQuery("delete from Person where _key = ?")
            .setArgs("FirstKey"));

        c.iterator();

        c = p.query(new SqlFieldsQuery("select _key, _val, * from Person order by id, _key"));

        List<List<?>> leftovers = c.getAll();

        assertEquals(3, leftovers.size());

        assertEqualsCollections(Arrays.asList("SecondKey", createPerson(2, "Joe", "Black"), 2, "Joe", "Black"),
            leftovers.get(0));

        assertEqualsCollections(Arrays.asList("k3", createPerson(3, "Sylvia", "Green"), 3, "Sylvia", "Green"),
            leftovers.get(1));

        assertEqualsCollections(Arrays.asList("f0u4thk3y", createPerson(4, "Jane", "Silver"), 4, "Jane", "Silver"),
            leftovers.get(2));
    }

    /**
     * In binary mode, this test checks that inner forcing of keepBinary works - without it, EntryProcessors
     * inside DML engine would compare binary and non-binary objects with the same keys and thus fail.
     */
    @Test
    public void testDeleteSimpleWithoutKeepBinary() {
        IgniteCache p = ignite(0).cache("S2P");

        QueryCursor<List<?>> c = p.query(new SqlFieldsQuery("delete from Person p where length(p._key) = 2 " +
            "or p.secondName like '%ite'"));

        c.iterator();

        c = p.query(new SqlFieldsQuery("select _key, _val, * from Person order by id"));

        List<List<?>> leftovers = c.getAll();

        assertEquals(2, leftovers.size());

        assertEqualsCollections(Arrays.asList("SecondKey", new Person(2, "Joe", "Black"), 2, "Joe", "Black"),
            leftovers.get(0));

        assertEqualsCollections(Arrays.asList("f0u4thk3y", new Person(4, "Jane", "Silver"), 4, "Jane", "Silver"),
            leftovers.get(1));
    }
}
