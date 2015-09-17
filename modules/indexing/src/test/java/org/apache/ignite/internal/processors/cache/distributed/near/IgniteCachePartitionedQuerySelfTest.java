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

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractQuerySelfTest;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CachePeekMode.ALL;

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
     * @throws Exception If failed.
     */
    public void testFieldsQuery() throws Exception {
        Person p1 = new Person("Jon", 1500);
        Person p2 = new Person("Jane", 2000);
        Person p3 = new Person("Mike", 1800);
        Person p4 = new Person("Bob", 1900);

        Ignite ignite0 = grid(0);

        IgniteCache<UUID, Person> cache0 = ignite0.cache(null);

        cache0.put(p1.id(), p1);
        cache0.put(p2.id(), p2);
        cache0.put(p3.id(), p3);
        cache0.put(p4.id(), p4);

        assertEquals(4, cache0.localSize(ALL));

        // Fields query
        QueryCursor<List<?>> qry = cache0
            .query(new SqlFieldsQuery("select name from Person where salary > ?").setArgs(1600));

        Collection<List<?>> res = qry.getAll();

        assertEquals(3, res.size());

        // Fields query count(*)
        qry = cache0.query(new SqlFieldsQuery("select count(*) from Person"));

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

        IgniteCache<UUID, Person> cache0 = grid(0).cache(null);

        cache0.put(p1.id(), p1);
        cache0.put(p2.id(), p2);
        cache0.put(p3.id(), p3);
        cache0.put(p4.id(), p4);

        assertEquals(4, cache0.localSize(ALL));

        assert grid(0).cluster().nodes().size() == gridCount();

        QueryCursor<Cache.Entry<UUID, Person>> qry =
            cache0.query(new SqlQuery<UUID, Person>(Person.class, "salary < 2000"));

        // Execute on full projection, duplicates are expected.
        Collection<Cache.Entry<UUID, Person>> entries = qry.getAll();

        assert entries != null;

        info("Queried entries: " + entries);

        // Expect result including backup persons.
        assertEquals(gridCount(), entries.size());

        checkResult(entries, p1, p3, p4);
    }

    /**
     * @param entries Queried result.
     * @param persons Persons that should be in the result.
     */
    private void checkResult(Iterable<Cache.Entry<UUID, Person>> entries, Person... persons) {
        for (Cache.Entry<UUID, Person> entry : entries) {
            assertEquals(entry.getKey(), entry.getValue().id());

            assert F.asList(persons).contains(entry.getValue());
        }
    }
}