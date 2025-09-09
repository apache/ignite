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
package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.SortOrder;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** */
public class IndexDdlIntegrationTest extends AbstractDdlIntegrationTest {
    /** Cache name. */
    private static final String CACHE_NAME = "my_cache";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        sql("create table my_table(id int, val_int int, val_str varchar) with cache_name=\"" + CACHE_NAME + "\"");
    }

    /**
     * Creates and drops index.
     */
    @Test
    public void createDropIndexSimpleCase() {
        assertNull(findIndex(CACHE_NAME, "my_index"));

        sql("create index my_index on my_table(id)");

        assertNotNull(findIndex(CACHE_NAME, "my_index"));

        sql("drop index my_index");

        assertNull(findIndex(CACHE_NAME, "my_index"));

        int cnt = indexes(CACHE_NAME).size();

        sql("create index on my_table(val_int)");

        assertEquals(cnt + 1, indexes(CACHE_NAME).size());
    }

    /**
     * Creates and drops index on not default schema.
     */
    @Test
    public void createDropIndexWithSchema() {
        String cacheName = "cache2";

        sql("create table my_schema.my_table2(id int) with cache_name=\"" + cacheName + "\"");

        assertNull(findIndex(cacheName, "my_index2"));

        sql("create index my_index2 on my_schema.my_table2(id)");

        assertNotNull(findIndex(cacheName, "my_index2"));

        GridTestUtils.assertThrowsAnyCause(log, () -> sql("drop index my_index2"), IgniteSQLException.class,
            "Index doesn't exist");

        assertNotNull(findIndex(cacheName, "my_index2"));

        sql("drop index my_schema.my_index2");

        assertNull(findIndex(cacheName, "my_index2"));
    }

    /**
     * Creates index with "if not exists" clause.
     */
    @Test
    public void createIndexWithIfNotExistsClause() {
        assertNull(findIndex(CACHE_NAME, "my_index"));

        sql("create index if not exists my_index on my_table(id)");

        GridTestUtils.assertThrowsAnyCause(log, () -> sql("create index my_index on my_table(val_int)"),
            IgniteSQLException.class, "Index already exists");

        assertNotNull(findIndex(CACHE_NAME, "my_index"));

        sql("create index if not exists my_index on my_table(val_str)");

        Index idx = findIndex(CACHE_NAME, "my_index");

        assertNotNull(idx);

        List<String> keys = new ArrayList<>(indexKeyDefinitions(idx).keySet());

        assertEquals("ID", keys.get(0));
    }

    /**
     * Creates drops index with "if exists" clause.
     */
    @Test
    public void dropIndexWithIfExistsClause() {
        assertNull(findIndex(CACHE_NAME, "my_index"));

        sql("create index my_index on my_table(id)");

        assertNotNull(findIndex(CACHE_NAME, "my_index"));

        sql("drop index if exists my_index");

        assertNull(findIndex(CACHE_NAME, "my_index"));

        sql("drop index if exists my_index");

        GridTestUtils.assertThrowsAnyCause(log, () -> sql("drop index my_index"), IgniteSQLException.class,
            "Index doesn't exist");
    }

    /**
     * Creates index with different columns ordering.
     */
    @Test
    public void createIndexWithColumnsOrdering() {
        assertNull(findIndex(CACHE_NAME, "my_index"));

        sql("create index my_index on my_table(id, val_int asc, val_str desc)");

        Index idx = findIndex(CACHE_NAME, "my_index");

        assertNotNull(idx);

        LinkedHashMap<String, IndexKeyDefinition> keyDefs = indexKeyDefinitions(idx);
        List<String> keys = new ArrayList<>(keyDefs.keySet());

        assertEquals("ID", keys.get(0));
        assertEquals(SortOrder.ASC, keyDefs.get(keys.get(0)).order().sortOrder());
        assertEquals("VAL_INT", keys.get(1));
        assertEquals(SortOrder.ASC, keyDefs.get(keys.get(1)).order().sortOrder());
        assertEquals("VAL_STR", keys.get(2));
        assertEquals(SortOrder.DESC, keyDefs.get(keys.get(2)).order().sortOrder());
    }

    /**
     * Creates index with inline size.
     */
    @Test
    public void createIndexWithInlineSize() {
        assertNull(findIndex(CACHE_NAME, "my_index"));

        sql("create index my_index on my_table(val_str) inline_size 10");

        Index idx = findIndex(CACHE_NAME, "my_index");

        assertNotNull(idx);

        InlineIndex inlineIdx = idx.unwrap(InlineIndex.class);

        assertNotNull(inlineIdx);
        assertEquals(10, inlineIdx.inlineSize());
    }

    /**
     * Creates index with inline size.
     */
    @Test
    public void createIndexWithParallel() {
        assertNull(findIndex(CACHE_NAME, "my_index"));

        sql("create index my_index on my_table(val_str) parallel 10");

        assertNotNull(findIndex(CACHE_NAME, "my_index"));
    }

    /** */
    private Index findIndex(String cacheName, String idxName) {
        return F.find(indexes(cacheName), null, (IgnitePredicate<Index>)i -> idxName.equalsIgnoreCase(i.name()));
    }

    /** */
    private Collection<Index> indexes(String cacheName) {
        IgniteEx node = grid(0);

        return node.context().indexProcessor().indexes(cacheName);
    }

    /** */
    private LinkedHashMap<String, IndexKeyDefinition> indexKeyDefinitions(Index idx) {
        return grid(0).context().indexProcessor().indexDefinition(idx.id()).indexKeyDefinitions();
    }
}
