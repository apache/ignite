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

package org.apache.ignite.cache.query;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;

/** */
@RunWith(Parameterized.class)
public class IndexQueryAliasTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final String ID_IDX = "ID_IDX";

    /** */
    private static final String DESC_ID_IDX = "DESC_ID_IDX";

    /** */
    private static final int CNT = 10_000;

    /** Query index, {@code null} or index name. */
    @Parameterized.Parameter
    public String qryIdx;

    /** Query desc index, {@code null} or index name. */
    @Parameterized.Parameter(1)
    public String qryDescIdx;

    /** */
    @Parameterized.Parameters(name = "qryIdx={0}, qryDescIdx={1}")
    public static List<Object[]> params() {
        return F.asList(
            new Object[] {null, null},
            new Object[] {ID_IDX, DESC_ID_IDX}
        );
    }

    /** */
    private static IgniteCache<Long, Person> cache;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite crd = startGrids(2);

        cache = crd.cache(CACHE);

        for (int i = 0; i < CNT; i++)
            cache.put((long)i, new Person(i));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        QueryIndex idIdx = new QueryIndex("id", true, ID_IDX);
        QueryIndex descIdIdx = new QueryIndex("descId", false, DESC_ID_IDX);

        QueryEntity e = new QueryEntity(Long.class.getName(), Person.class.getName())
            .setFields(new LinkedHashMap<>(
                F.asMap("id", Integer.class.getName(), "descId", Integer.class.getName()))
            )
            .setAliases(F.asMap("id", "asId", "descId", "asDescId"))
            .setIndexes(Arrays.asList(idIdx, descIdIdx));

        CacheConfiguration<?, ?> ccfg1 = new CacheConfiguration<>()
            .setName(CACHE)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setQueryEntities(Collections.singletonList(e));

        cfg.setCacheConfiguration(ccfg1);

        return cfg;
    }

    /** */
    @Test
    public void testAliasRangeQueries() {
        int pivot = new Random().nextInt(CNT);

        // Lt.
        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, qryIdx)
            .setCriteria(lt("asId", pivot));

        check(cache.query(qry), 0, pivot, false);

        // Lt, desc index.
        IndexQuery<Long, Person> descQry = new IndexQuery<Long, Person>(Person.class, qryDescIdx)
            .setCriteria(lt("asDescId", pivot));

        check(cache.query(descQry), 0, pivot, true);
    }

    /** */
    @Test
    public void testAliasCaseRangeQueries() {
        int pivot = new Random().nextInt(CNT);

        String idIdx = qryIdx != null ? qryIdx.toLowerCase() : null;

        // Lt.
        IndexQuery<Long, Person> qry = new IndexQuery<Long, Person>(Person.class, idIdx)
            .setCriteria(lt("ASID", pivot));

        check(cache.query(qry), 0, pivot, false);

        String idDescIdx = qryDescIdx != null ? qryDescIdx.toLowerCase() : null;

        // Lt, desc index.
        IndexQuery<Long, Person> descQry = new IndexQuery<Long, Person>(Person.class, idDescIdx)
            .setCriteria(lt("ASDESCID", pivot));

        check(cache.query(descQry), 0, pivot, true);
    }

    /**
     * @param left First cache key, inclusive.
     * @param right Last cache key, exclusive.
     */
    private void check(QueryCursor<Cache.Entry<Long, Person>> cursor, int left, int right, boolean desc) {
        List<Cache.Entry<Long, Person>> all = cursor.getAll();

        assertEquals(right - left, all.size());

        for (int i = 0; i < all.size(); i++) {
            int expKey = desc ? right - 1 - i : i;

            assertEquals(new Person(expKey), all.get(i).getValue());
        }
    }

    /** */
    private static class Person {
        /** */
        @GridToStringInclude
        final int id;

        /** */
        @GridToStringInclude
        final int descId;

        /** */
        Person(int id) {
            this.id = id;
            descId = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person)o;

            return Objects.equals(id, person.id) && Objects.equals(descId, person.descId);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, descId);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Person.class, this);
        }
    }
}
