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

/**
 *
 */
@SuppressWarnings("unchecked")
public class IgniteCacheUpdateSqlQuerySelfTest extends IgniteCacheAbstractSqlDmlQuerySelfTest {
    /**
     *
     */
    public void testUpdateSimple() {
        IgniteCache p = cache();

        QueryCursor<List<?>> c = p.query(new SqlFieldsQuery("update Person p set p.id = p.id * 2, p.name = " +
            "substring(p.name, 0, 2) where length(p._key) = ? or p.secondName like ?").setArgs(2, "%ite"));

        c.iterator();

        c = p.query(new SqlFieldsQuery("select * from Person order by _key, id"));

        List<List<?>> leftovers = c.getAll();

        assertEquals(4, leftovers.size());

        assertEqualsCollections(Arrays.asList("FirstKey", createPerson(2, "Jo", "White"), 2, "Jo", "White"),
            leftovers.get(0));

        assertEqualsCollections(Arrays.asList("SecondKey", createPerson(2, "Joe", "Black"), 2, "Joe", "Black"),
            leftovers.get(1));

        assertEqualsCollections(Arrays.asList("f0u4thk3y", createPerson(4, "Jane", "Silver"), 4, "Jane", "Silver"),
            leftovers.get(2));

        assertEqualsCollections(Arrays.asList("k3", createPerson(6, "Sy", "Green"), 6, "Sy", "Green"),
            leftovers.get(3));
    }

    /**
     *
     */
    public void testUpdateSingle() {
        IgniteCache p = cache();

        QueryCursor<List<?>> c = p.query(new SqlFieldsQuery("update Person p set _val = ? where _key = ?")
            .setArgs(createPerson(2, "Jo", "White"), "FirstKey"));

        c.iterator();

        c = p.query(new SqlFieldsQuery("select * from Person order by id, _key"));

        List<List<?>> leftovers = c.getAll();

        assertEquals(4, leftovers.size());

        assertEqualsCollections(Arrays.asList("FirstKey", createPerson(2, "Jo", "White"), 2, "Jo", "White"),
            leftovers.get(0));

        assertEqualsCollections(Arrays.asList("SecondKey", createPerson(2, "Joe", "Black"), 2, "Joe", "Black"),
            leftovers.get(1));

        assertEqualsCollections(Arrays.asList("k3", createPerson(3, "Sylvia", "Green"), 3, "Sylvia", "Green"),
            leftovers.get(2));

        assertEqualsCollections(Arrays.asList("f0u4thk3y", createPerson(4, "Jane", "Silver"), 4, "Jane", "Silver"),
            leftovers.get(3));
    }

    /**
     *
     */
    public void testUpdateValueAndFields() {
        IgniteCache p = cache();

        QueryCursor<List<?>> c = p.query(new SqlFieldsQuery("update Person p set id = ?, _val = ? where _key = ?")
            .setArgs(44, createPerson(2, "Jo", "Woo"), "FirstKey"));

        c.iterator();

        c = p.query(new SqlFieldsQuery("select * from Person order by _key, id"));

        List<List<?>> leftovers = c.getAll();

        assertEquals(4, leftovers.size());

        assertEqualsCollections(Arrays.asList("FirstKey", createPerson(44, "Jo", "Woo"), 44, "Jo", "Woo"),
            leftovers.get(0));

        assertEqualsCollections(Arrays.asList("SecondKey", createPerson(2, "Joe", "Black"), 2, "Joe", "Black"),
            leftovers.get(1));

        assertEqualsCollections(Arrays.asList("f0u4thk3y", createPerson(4, "Jane", "Silver"), 4, "Jane", "Silver"),
            leftovers.get(2));

        assertEqualsCollections(Arrays.asList("k3", createPerson(3, "Sylvia", "Green"), 3, "Sylvia", "Green"),
            leftovers.get(3));
    }

    /**
     *
     */
    public void testDefault() {
        IgniteCache p = cache();

        QueryCursor<List<?>> c = p.query(new SqlFieldsQuery("update Person p set id = DEFAULT, _val = ? where _key = ?")
            .setArgs(createPerson(2, "Jo", "Woo"), "FirstKey"));

        c.iterator();

        c = p.query(new SqlFieldsQuery("select * from Person order by _key, id"));

        List<List<?>> leftovers = c.getAll();

        assertEquals(4, leftovers.size());

        assertEqualsCollections(Arrays.asList("FirstKey", createPerson(0, "Jo", "Woo"), 0, "Jo", "Woo"),
            leftovers.get(0));

        assertEqualsCollections(Arrays.asList("SecondKey", createPerson(2, "Joe", "Black"), 2, "Joe", "Black"),
            leftovers.get(1));

        assertEqualsCollections(Arrays.asList("f0u4thk3y", createPerson(4, "Jane", "Silver"), 4, "Jane", "Silver"),
            leftovers.get(2));

        assertEqualsCollections(Arrays.asList("k3", createPerson(3, "Sylvia", "Green"), 3, "Sylvia", "Green"),
            leftovers.get(3));
    }
}
