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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.*;

import javax.cache.Cache;
import java.util.*;

import static org.apache.ignite.cache.CacheMode.*;

/**
 * Tests for partitioned cache queries.
 */
public class IgniteCachePartitionedQuerySelfTest extends IgniteCacheAbstractQuerySelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testSingleNodeQuery() throws Exception {
        Person p1 = new Person("Jon", 1500);
        Person p2 = new Person("Jane", 2000);
        Person p3 = new Person("Mike", 1800);
        Person p4 = new Person("Bob", 1900);

        IgniteCache<UUID, Person> cache0 = grid(0).jcache(null);

        cache0.put(p1.id(), p1);
        cache0.put(p2.id(), p2);
        cache0.put(p3.id(), p3);
        cache0.put(p4.id(), p4);

        assertEquals(4, cache0.size());

        QueryCursor<Cache.Entry<UUID, Person>> qry =
            cache0.localQuery(new QuerySql(Person.class, "salary < 2000"));

        Collection<Cache.Entry<UUID, Person>> entries = qry.getAll();

        assert entries != null;

        assertEquals(3, entries.size());

        checkResult(entries, p1, p3, p4);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFieldsQuery() throws Exception {
        Person p1 = new Person("Jon", 1500);
        Person p2 = new Person("Jane", 2000);
        Person p3 = new Person("Mike", 1800);
        Person p4 = new Person("Bob", 1900);

        Ignite ignite0 = grid(0);

        IgniteCache<UUID, Person> cache0 = ignite0.jcache(null);

        cache0.put(p1.id(), p1);
        cache0.put(p2.id(), p2);
        cache0.put(p3.id(), p3);
        cache0.put(p4.id(), p4);

        assertEquals(4, cache0.size());

        // Fields query
        QueryCursor<List<?>> qry = cache0
            .queryFields(new QuerySql("select name from Person where salary > ?").setArgs(1600));

        Collection<List<?>> res = qry.getAll();

        assertEquals(3, res.size());

        // Fields query count(*)
        qry = cache0.queryFields(new QuerySql("select count(*) from Person"));

        res = qry.getAll();

        int cnt = 0;

        for (List<?> row : res)
            cnt += (Long)row.get(0);

        assertEquals(4, cnt);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleNodesQuery() throws Exception {
        Person p1 = new Person("Jon", 1500);
        Person p2 = new Person("Jane", 2000);
        Person p3 = new Person("Mike", 1800);
        Person p4 = new Person("Bob", 1900);

        IgniteCache<UUID, Person> cache0 = grid(0).jcache(null);

        cache0.put(p1.id(), p1);
        cache0.put(p2.id(), p2);
        cache0.put(p3.id(), p3);
        cache0.put(p4.id(), p4);

        assertEquals(4, cache0.size());

        assert grid(0).nodes().size() == gridCount();

        QueryCursor<Cache.Entry<UUID, Person>> qry =
            cache0.query(new QuerySql(Person.class, "salary < 2000"));

        // Execute on full projection, duplicates are expected.
        Collection<Cache.Entry<UUID, Person>> entries = qry.getAll();

        assert entries != null;

        info("Queried entries: " + entries);

        // Expect result including backup persons.
        assertEquals(3 * gridCount(), entries.size());

        checkResult(entries, p1, p3, p4);
    }

    /**
     * @param entries Queried result.
     * @param persons Persons that should be in the result.
     */
    private void checkResult(Iterable<Cache.Entry<UUID, Person>> entries, Person... persons) {
        for (Cache.Entry<UUID, Person> entry : entries) {
            assertEquals(entry.getKey(), entry.getValue().id());

            assert F.<Person>asList(persons).contains(entry.getValue());
        }
    }
}
