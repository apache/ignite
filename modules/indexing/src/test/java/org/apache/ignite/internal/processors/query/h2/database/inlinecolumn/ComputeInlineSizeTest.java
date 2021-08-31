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

import java.util.Arrays;
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
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.junit.Test;

/** Tests for the computation inline size. */
public class ComputeInlineSizeTest extends AbstractIndexingCommonTest {
    /** */
    private static final String CACHE = "CACHE";

    /** */
    private static IgniteEx ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ignite = startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite.destroyCache(CACHE);
    }

    /** */
    @Test
    public void testAnnotationPrecision() {
        CacheConfiguration<Long, Person> ccfg = new CacheConfiguration<Long, Person>()
            .setName(CACHE)
            .setIndexedTypes(Long.class, Person.class);

        ignite.createCache(ccfg);

        checkIdxsInlineSizes();
    }

    /** */
    @Test
    public void testSQLIndexes() {
        StringBuilder bld = new StringBuilder();

        String createQry = "create table TABLE (" +
            "id long primary key" +
            ", str varchar" +
            ", bytes binary" +
            ", strprec varchar(" + (InlineIndexTree.IGNITE_VARIABLE_TYPE_DEFAULT_INLINE_SIZE + 10) + ")" +
            ", strprecbig varchar(" + (PageIO.MAX_PAYLOAD_SIZE * 2) + " )" +
            ", bytesprec binary(" + (InlineIndexTree.IGNITE_VARIABLE_TYPE_DEFAULT_INLINE_SIZE + 20) + " )" +
            ")with \"cache_name=" + CACHE + "\";";

        bld.append(createQry);

        for (String s: Arrays.asList("str", "strprec", "bytes", "bytesprec", "strprecbig"))
            bld.append(String.format("create index PERSON_%s_IDX on TABLE (%s); ", s.toUpperCase(), s));

        query(new SqlFieldsQuery(bld.toString()));

        checkIdxsInlineSizes();
    }

    /** */
    private void checkIdxsInlineSizes() {
        Collection<Index> idx = ignite.context().indexProcessor().indexes(context());

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
            InlineIndexImpl impl = (InlineIndexImpl)i;

            if (expInlineSize.containsKey(impl.name())) {
                int inlineSize = expInlineSize.remove(impl.name());

                assertEquals(inlineSize, impl.inlineSize());
            }
        }

        assertTrue(expInlineSize.isEmpty());
    }

    /** */
    private void query(SqlFieldsQuery qry) {
        ignite.context().query().querySqlFields(qry, false, false);
    }

    /** */
    private GridCacheContext<Long, Person> context() {
        IgniteInternalCache<Long, Person> c = ignite.cachex(CACHE);

        return c.context();
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

        /** */
        @QuerySqlField(index = true, precision = InlineIndexTree.IGNITE_VARIABLE_TYPE_DEFAULT_INLINE_SIZE + 20)
        private byte[] bytesPrec;
    }
}
