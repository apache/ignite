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

package org.apache.ignite.internal.processors.query.h2.database.inlinecolumn;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.processors.cache.GatewayProtectedCacheProxy;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.junit.Test;

/** Tests for the computation inline size. */
public class ComputeInlineSizeTest extends AbstractIndexingCommonTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void testAnnotaitionPrecision() throws Exception {
        IgniteEx ignite = startGrid();

        CacheConfiguration ccfg = new CacheConfiguration<>()
            .setName("TEST_CACHE")
            .setIndexedTypes(Long.class, Person.class);

        GatewayProtectedCacheProxy cache = (GatewayProtectedCacheProxy) ignite.createCache(ccfg);

        checkIdxsInlineSizes(ignite, cache.context());
    }

    /** */
    @Test
    public void testSQLIndexes() throws Exception {
        IgniteEx ignite = startGrid();

        GatewayProtectedCacheProxy cache = (GatewayProtectedCacheProxy) ignite.createCache(
            new CacheConfiguration<>().setName("CACHE"));

        SqlFieldsQuery qry = new SqlFieldsQuery("create table TABLE (" +
            "id long primary key" +
            ", str varchar" +
            ", bytes binary" +
            ", strprec varchar(" + (InlineIndexTree.IGNITE_VARIABLE_TYPE_DEFAULT_INLINE_SIZE + 10) + ")" +
            ", strprecbig varchar(" + (PageIO.MAX_PAYLOAD_SIZE * 2) + " )" +
            ", bytesprec binary(" + (InlineIndexTree.IGNITE_VARIABLE_TYPE_DEFAULT_INLINE_SIZE + 20) + " )" +
            ")with \"cache_name=TEST_CACHE_TABLE\";");
        cache.query(qry);

        qry = new SqlFieldsQuery("create index PERSON_STR_IDX on TABLE (str);");
        cache.query(qry);

        qry = new SqlFieldsQuery("create index PERSON_STRPREC_IDX on TABLE (strprec);");
        cache.query(qry);

        qry = new SqlFieldsQuery("create index PERSON_BYTES_IDX on TABLE (bytes);");
        cache.query(qry);

        qry = new SqlFieldsQuery("create index PERSON_BYTESPREC_IDX on TABLE (bytesprec);");
        cache.query(qry);

        qry = new SqlFieldsQuery("create index PERSON_STRPRECBIG_IDX on TABLE (strprecbig);");
        cache.query(qry);

        checkIdxsInlineSizes(ignite, ((GatewayProtectedCacheProxy) ignite.cache("TEST_CACHE_TABLE")).context());
    }

    /** */
    private void checkIdxsInlineSizes(IgniteEx ignite, GridCacheContext cctx) {
        Collection<Index> idx = ignite.context().indexProcessor().indexes(cctx);

        Map<String, Integer> expInlineSize = new HashMap<String, Integer>() {{
            // 9 is inline for _KEY (LongIndexKeyType).
            put("PERSON_STR_IDX",
                9 + InlineIndexTree.IGNITE_VARIABLE_TYPE_DEFAULT_INLINE_SIZE);
            put("PERSON_STRPRECBIG_IDX",
                InlineIndexTree.IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT);
            // 3 is for storing info (type, length) of inlined key.
            put("PERSON_STRPREC_IDX",
                9 + InlineIndexTree.IGNITE_VARIABLE_TYPE_DEFAULT_INLINE_SIZE + 10 + 3);
            put("PERSON_BYTES_IDX",
                9 + InlineIndexTree.IGNITE_VARIABLE_TYPE_DEFAULT_INLINE_SIZE);
            put("PERSON_BYTESPREC_IDX",
                9 + InlineIndexTree.IGNITE_VARIABLE_TYPE_DEFAULT_INLINE_SIZE + 20 + 3);
        }};

        for (Index i: idx) {
            InlineIndexImpl impl = (InlineIndexImpl) i;

            if (expInlineSize.containsKey(impl.name())) {
                int inlineSize = expInlineSize.remove(impl.name());

                assertEquals(inlineSize, impl.inlineSize());
            }
        }

        assertTrue(expInlineSize.isEmpty());
    }

    /** */
    public static class Person {
        /** */
        @QuerySqlField(index = true)
        private String str;

        /** */
        @QuerySqlField(index = true, precision = InlineIndexTree.IGNITE_VARIABLE_TYPE_DEFAULT_INLINE_SIZE + 10)
        private String strPrec;

        /** */
        @QuerySqlField(index = true, precision = PageIO.MAX_PAYLOAD_SIZE * 2)
        private String strPrecBig;

        /** */
        @QuerySqlField(index = true)
        private byte[] bytes;

        // TODO: precision and ordered groups.
        /** */
        @QuerySqlField(index = true, precision = InlineIndexTree.IGNITE_VARIABLE_TYPE_DEFAULT_INLINE_SIZE + 20)
        private byte[] bytesPrec;
    }
}
