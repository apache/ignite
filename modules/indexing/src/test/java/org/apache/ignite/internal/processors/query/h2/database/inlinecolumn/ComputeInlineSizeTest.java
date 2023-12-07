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
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.BooleanInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.ByteInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.DoubleInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.FloatInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.IntegerInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.LongInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.ShortInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.TimeInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.TimestampInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.UuidInlineIndexKeyType;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiTuple;
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

        query(bld.toString());

        checkIdxsInlineSizes();
    }

    /** */
    @Test
    public void testTooBigInlineNotUsed() {
        InlineIndexKeyType idIdxType = new LongInlineIndexKeyType();

        Collection<IgniteBiTuple<String, InlineIndexKeyType>> fixLenTypes = Arrays.asList(
            F.t("BOOLEAN", new BooleanInlineIndexKeyType()),
            F.t("TINYINT", new ByteInlineIndexKeyType()),
            F.t("DATE", new DateInlineIndexKeyType()),
            F.t("DOUBLE", new DoubleInlineIndexKeyType()),
            F.t("REAL", new FloatInlineIndexKeyType()),
            F.t("INT", new IntegerInlineIndexKeyType()),
            F.t("BIGINT", new LongInlineIndexKeyType()),
            F.t("SMALLINT", new ShortInlineIndexKeyType()),
            F.t("TIME", new TimeInlineIndexKeyType()),
            F.t("TIMESTAMP", new TimestampInlineIndexKeyType()),
            F.t("UUID", new UuidInlineIndexKeyType())
        );

        StringBuilder tbl = new StringBuilder("CREATE TABLE T1 (ID LONG PRIMARY KEY, _VARCHAR VARCHAR");

        for (IgniteBiTuple<String, InlineIndexKeyType> type : fixLenTypes)
            tbl.append(String.format(", _%s %s", type.get1(), type.get1()));

        tbl.append(")");

        query(tbl.toString());

        checkIndexInlineSize("VARCHAR", 1000);

        for (IgniteBiTuple<String, InlineIndexKeyType> type0 : fixLenTypes) {
            checkIndexInlineSize(type0.get1(), type0.get2().inlineSize() + idIdxType.inlineSize());

            checkIndexInlineSize(type0.get1() + ", _VARCHAR", 1000);

            for (IgniteBiTuple<String, InlineIndexKeyType> type1 : fixLenTypes) {
                if (type0 == type1)
                    continue;

                checkIndexInlineSize(
                    type0.get1() + ", _" + type1.get1(),
                    type0.get2().inlineSize() + type1.get2().inlineSize() + idIdxType.inlineSize()
                );
            }
        }
    }

    /** */
    private void checkIndexInlineSize(String cols, int expInlineSz) {
        query(String.format("CREATE INDEX IDX1 ON T1(_%s) INLINE_SIZE 1000", cols));

        InlineIndexImpl idx = (InlineIndexImpl)
            ignite.context().indexProcessor().index(new IndexName("SQL_PUBLIC_T1", "PUBLIC", "T1", "IDX1"));

        assertEquals(cols, expInlineSz, idx.inlineSize());

        query("DROP INDEX IDX1");
    }

    /** */
    private void checkIdxsInlineSizes() {
        Collection<Index> idx = ignite.context().indexProcessor().indexes(CACHE);

        Map<String, Integer> expInlineSize = new HashMap<>();

        // 9 is inline for _KEY (LongIndexKeyType).
        expInlineSize.put("PERSON_STR_IDX",
            9 + InlineIndexTree.IGNITE_VARIABLE_TYPE_DEFAULT_INLINE_SIZE);
        expInlineSize.put("PERSON_STRPRECBIG_IDX",
            InlineIndexTree.IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT);
        // 3 is for storing info (type, length) of inlined key.
        expInlineSize.put("PERSON_STRPREC_IDX",
            9 + InlineIndexTree.IGNITE_VARIABLE_TYPE_DEFAULT_INLINE_SIZE + 10 + 3);
        expInlineSize.put("PERSON_BYTES_IDX",
            9 + InlineIndexTree.IGNITE_VARIABLE_TYPE_DEFAULT_INLINE_SIZE);
        expInlineSize.put("PERSON_BYTESPREC_IDX",
            9 + InlineIndexTree.IGNITE_VARIABLE_TYPE_DEFAULT_INLINE_SIZE + 20 + 3);

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
    private void query(String qry) {
        ignite.context().query().querySqlFields(new SqlFieldsQuery(qry), false, false);
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
