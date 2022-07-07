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

import java.math.BigDecimal;
import java.util.List;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.database.H2TreeIndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gt;
import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.lt;

/** */
@RunWith(Parameterized.class)
public class IndexQueryInlineSizesTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "TEST_CACHE";

    /** */
    private static final String TABLE_CACHE = "TEST_CACHE_TABLE";

    /** */
    private static final String VALUE_TYPE = "TEST_VALUE_TYPE";

    /** */
    private static final String TABLE = "TEST_TABLE";

    /** */
    private static final int CNT = 10_000;

    /** */
    private static IgniteEx crd;

    /** */
    @Parameterized.Parameter
    public int inlineSize;

    /** */
    @Parameterized.Parameters(name = "inlineSize={0}")
    public static Iterable<Integer> parameters() {
        return IntStream.range(0, 20).boxed().collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        crd = startGrid(2);

        prepareTable(crd);
    }

    /** */
    @Test
    public void testFixedInlineKeys() throws Exception {
        // fld1 int, fld2 int, _key int. Inline int size is 5 (1 tag, 4 value) -> full inlined value is 15 bytes.
        // If INLINE_SIZE > 15 it will be ignored. Real inline size will be 15 bytes.
        try (Index idx = new Index(inlineSize, "fld1, fld2", inlineSize > 15 ? 15 : inlineSize)) {
            check((low, high) -> new IndexQuery<Integer, BinaryObject>(VALUE_TYPE, idx.idxName)
                .setCriteria(gt("fld1", low), lt("fld2", high)));
        }
    }

    /** */
    @Test
    public void testVarInlineKeys() throws Exception {
        try (Index idx = new Index(inlineSize, "fld1, fld3")) {
            check((low, high) -> new IndexQuery<Integer, BinaryObject>(VALUE_TYPE, idx.idxName)
                .setCriteria(gt("fld1", low), lt("fld3", strFieldVal(high))));
        }
    }

    /** */
    @Test
    public void testVarInlineKeysFirst() throws Exception {
        try (Index idx = new Index(inlineSize, "fld3, fld1")) {
            check((low, high) -> new IndexQuery<Integer, BinaryObject>(VALUE_TYPE, idx.idxName)
                .setCriteria(gt("fld1", low), lt("fld3", strFieldVal(high))));
        }
    }

    /** */
    @Test
    public void testNonInlinedKeys() throws Exception {
        // fld1 int can be inlined, fld4 decimal can't be inlined.
        // Maximum possible inline size for fld1 is 5 bytes.
        // If INLINE_SIZE > 5 it will be ignored. Real inline size will be 5 bytes.
        try (Index idx = new Index(inlineSize, "fld1, fld4", inlineSize > 5 ? 5 : inlineSize)) {
            check((low, high) -> new IndexQuery<Integer, BinaryObject>(VALUE_TYPE, idx.idxName)
                .setCriteria(gt("fld1", low), lt("fld4", new BigDecimal(high))));
        }
    }

    /** */
    @Test
    public void testNonInlinedKeysFirst() throws Exception {
        try (Index idx = new Index(inlineSize, "fld4, fld1", 0)) {
            check((low, high) -> new IndexQuery<Integer, BinaryObject>(VALUE_TYPE, idx.idxName)
                .setCriteria(gt("fld1", low), lt("fld4", new BigDecimal(high))));
        }
    }

    /** */
    @Test
    public void testVarlenAndNonInlined() throws Exception {
        try (Index idx = new Index(inlineSize, "fld3, fld4")) {
            check((low, high) -> new IndexQuery<Integer, BinaryObject>(VALUE_TYPE, idx.idxName)
                .setCriteria(gt("fld3", strFieldVal(low)), lt("fld4", new BigDecimal(high))));
        }
    }

    /** */
    private void check(BiFunction<Integer, Integer, IndexQuery<Integer, BinaryObject>> qryBld) {
        Random r = new Random();

        int low = r.nextInt(CNT / 2);
        int high = low + r.nextInt(CNT / 2);

        IndexQuery<Integer, BinaryObject> qry = qryBld.apply(low, high);

        List<?> result = crd.cache(TABLE_CACHE).withKeepBinary().query(qry).getAll();

        assertEquals(high - low - 1, result.size());
    }

    /** */
    private void prepareTable(Ignite crd) {
        SqlFieldsQuery tblQry = new SqlFieldsQuery("create table " + TABLE +
            "(id int PRIMARY KEY, fld1 int, fld2 int, fld3 varchar, fld4 decimal)" +
            " with \"CACHE_NAME=" + TABLE_CACHE + ",VALUE_TYPE=" + VALUE_TYPE + "\";");

        crd.getOrCreateCache(CACHE).query(tblQry);

        try (IgniteDataStreamer<Integer, BinaryObject> s = crd.dataStreamer(TABLE_CACHE)) {
            IntStream.range(0, CNT).forEach((v) -> {
                BinaryObject bo = crd.binary().builder(VALUE_TYPE)
                    .setField("fld1", v)
                    .setField("fld2", v)
                    .setField("fld3", strFieldVal(v))
                    .setField("fld4", new BigDecimal(v))
                    .build();

                s.addData(v, bo);
            });
        }
    }

    /** */
    private static String strFieldVal(int val) {
        return String.format("%10s", val).replace(" ", "0");
    }

    /** */
    private static class Index implements AutoCloseable {
        /** */
        private final String idxName;

        /** */
        private Index(int inlineSize, String flds) {
            this(inlineSize, flds, inlineSize);
        }

        /** */
        private Index(int inlineSize, String flds, int expInlineSize) {
            idxName = "IDX_" + inlineSize;

            SqlFieldsQuery idxQry = new SqlFieldsQuery(
                "create index " + idxName + " on " + TABLE + "(" + flds + ") INLINE_SIZE " + inlineSize);

            crd.cache(CACHE).query(idxQry).getAll();

            GridH2Table tbl = ((IgniteH2Indexing)crd.context().query().getIndexing()).schemaManager()
                .dataTable("PUBLIC", TABLE);

            assertEquals(expInlineSize, ((H2TreeIndexBase)tbl.getIndex(idxName)).inlineSize());
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            SqlFieldsQuery idxQry = new SqlFieldsQuery("drop index " + idxName);

            crd.cache(CACHE).query(idxQry).getAll();
        }
    }
}
