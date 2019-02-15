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
