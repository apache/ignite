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

import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;

/** */
public class IndexQueryWrongIndexTest extends GridCommonAbstractTest {
    /** */
    private static final String ID_IDX = "ID_IDX";

    /** */
    private static final String DESC_ID_IDX = "DESC_ID_IDX";

    /** */
    @Test
    public void testWrongIndexAndFieldsMatching() throws Exception {
        Ignite crd = startGrids(2);

        IgniteCache<Integer, Person> cache = crd.getOrCreateCache(new CacheConfiguration<Integer, Person>()
            .setName("CACHE")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setIndexedTypes(Integer.class, Person.class));

        // Wrong fields in query.
        GridTestUtils.assertThrows(null, () -> {
            IndexQuery<Integer, Person> wrongQry = new IndexQuery<Integer, Person>(Person.class, DESC_ID_IDX)
                .setCriteria(lt("id", Integer.MAX_VALUE));

                cache.query(wrongQry).getAll();

                return null;
        }, CacheException.class, null);

        // Wrong fields in query.
        GridTestUtils.assertThrows(null, () -> {
            IndexQuery<Integer, Person> wrongQry = new IndexQuery<Integer, Person>(Person.class, ID_IDX)
                .setCriteria(lt("descId", Integer.MAX_VALUE));

            cache.query(wrongQry).getAll();

            return null;
        }, CacheException.class, null);
    }

    /** */
    private static class Person {
        /** */
        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = ID_IDX, order = 0))
        final int id;

        /** */
        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = DESC_ID_IDX, order = 0))
        final int descId;

        /** */
        Person(int id) {
            this.id = id;
            descId = id;
        }
    }
}
