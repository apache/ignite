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

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.BytesInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.LongInlineIndexColumn;
import org.apache.ignite.internal.processors.query.h2.database.inlinecolumn.StringInlineIndexColumn;
import org.h2.table.Column;
import org.h2.value.Value;
import org.junit.Test;

/** Tests for the computation of default inline size. */
public class H2ComputeInlineSizeTest extends AbstractIndexingCommonTest {

    /**
     * Test to check calculation of the default size for {@link StringInlineIndexColumn}.
     *
     * Steps:
     * 1) Call {@link H2TreeIndexBase#computeInlineSize(java.util.List, int, int)} function for {@link StringInlineIndexColumn}.
     * 2) Check that computed size is equal to default length for variable types.
     */
    @Test
    public void testDefaultSizeForString() {
        Column c = new Column("c", Value.STRING);

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(new StringInlineIndexColumn(c, false));

        assertEquals(H2TreeIndexBase.IGNITE_VARIABLE_TYPE_DEFAULT_INDEX_SIZE, H2TreeIndexBase.computeInlineSize(inlineIdxs, -1, -1));
    }

    /**
     * Test to check calculation of the default size for {@link BytesInlineIndexColumn}.
     *
     * Steps:
     * 1) Call {@link H2TreeIndexBase#computeInlineSize(java.util.List, int, int)} function for {@link BytesInlineIndexColumn}.
     * 2) Check that computed size is equal to default length for variable types.
     */
    @Test
    public void testDefaultSizeForByteArray() {
        Column c = new Column("c", Value.BYTES);

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(new BytesInlineIndexColumn(c, false));

        assertEquals(H2TreeIndexBase.IGNITE_VARIABLE_TYPE_DEFAULT_INDEX_SIZE, H2TreeIndexBase.computeInlineSize(inlineIdxs, -1, -1));
    }

    /**
     * Test to check calculation of the default size for {@link StringInlineIndexColumn} with defined length.
     *
     * Steps:
     * 1) Call {@link H2TreeIndexBase#computeInlineSize(java.util.List, int, int)} function for {@link StringInlineIndexColumn} with defined length.
     * 2) Check that computed size is equal to defined length + 3 bytes (inner system info for String type).
     */
    @Test
    public void testDefaultSizeForStringWithDefinedLength() {
        final byte LEN = H2TreeIndexBase.IGNITE_VARIABLE_TYPE_DEFAULT_INDEX_SIZE + 10;

        Column c = new Column("c", Value.STRING);
        c.setOriginalSQL("VARCHAR(" + LEN + ")");

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(new StringInlineIndexColumn(c, false));

        assertEquals(LEN + 3, H2TreeIndexBase.computeInlineSize(inlineIdxs, -1, -1));
    }

    /**
     * Test to check calculation of the default size for {@link BytesInlineIndexColumn} with defined length.
     *
     * Steps:
     * 1) Call {@link H2TreeIndexBase#computeInlineSize(java.util.List, int, int)} function for {@link BytesInlineIndexColumn} with defined length.
     * 2) Check that computed size is equal to defined length + 3 bytes (inner system info for byte[] type).
     */
    @Test
    public void testDefaultSizeForBytesWithDefinedLength() {
        final byte LEN = H2TreeIndexBase.IGNITE_VARIABLE_TYPE_DEFAULT_INDEX_SIZE + 20;

        Column c = new Column("c", Value.BYTES);
        c.setOriginalSQL("BINARY(" + LEN + ")");

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(new BytesInlineIndexColumn(c, false));

        assertEquals(LEN + 3, H2TreeIndexBase.computeInlineSize(inlineIdxs, -1, -1));
    }

    /**
     * Test to check calculation of the default size for {@link StringInlineIndexColumn} with unexpected sql pattern.
     *
     * Steps:
     * 1) Call {@link H2TreeIndexBase#computeInlineSize(java.util.List, int, int)} function for {@link StringInlineIndexColumn} with unexpected sql pattern.
     * 2) Check that computed size is equal to default length for variable types.
     */
    @Test
    public void testDefaultSizeForStringWithIncorrectSql() {
        Column c = new Column("c", Value.STRING);
        c.setOriginalSQL("CHAR()");

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(new StringInlineIndexColumn(c, false));

        assertEquals(H2TreeIndexBase.IGNITE_VARIABLE_TYPE_DEFAULT_INDEX_SIZE, H2TreeIndexBase.computeInlineSize(inlineIdxs, -1, -1));
    }

    /**
     * Test to check calculation of the default size for composite index.
     *
     * Steps:
     * 1) Call {@link H2TreeIndexBase#computeInlineSize(java.util.List, int, int)} function for {@link StringInlineIndexColumn},
     * {@link BytesInlineIndexColumn}, {@link LongInlineIndexColumn} and {@link StringInlineIndexColumn} with defined length.
     * 2) Check that computed size is equal to 2 * default length for variable types + constant length of long column +
     * defined String length + 3 bytes (inner system info for String type).
     */
    @Test
    public void testDefaultSizeForCompositeIndex() {
        final byte LEN = H2TreeIndexBase.IGNITE_VARIABLE_TYPE_DEFAULT_INDEX_SIZE - 1;

        Column c1 = new Column("c1", Value.STRING);
        Column c2 = new Column("c2", Value.BYTES);
        Column c3 = new Column("c3", Value.LONG);

        Column c4 = new Column("c4", Value.STRING);
        c4.setOriginalSQL("VARCHAR(" + LEN + ")");

        InlineIndexColumn lCol = new LongInlineIndexColumn(c3);

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(new StringInlineIndexColumn(c1, false));
        inlineIdxs.add(new BytesInlineIndexColumn(c2, false));
        inlineIdxs.add(lCol);
        inlineIdxs.add(new StringInlineIndexColumn(c4, false));

        assertEquals(2 * H2TreeIndexBase.IGNITE_VARIABLE_TYPE_DEFAULT_INDEX_SIZE + lCol.size() + 1 + LEN + 3,
            H2TreeIndexBase.computeInlineSize(inlineIdxs, -1, -1));
    }

    /**
     * Test to check calculation of the default size when calculated default size is larger than max default index size.
     *
     * Steps:
     * 1) Call {@link H2TreeIndexBase#computeInlineSize(java.util.List, int, int)} function for {@link StringInlineIndexColumn} with large length.
     * 2) Check that computed size is equal to default max length.
     */
    @Test
    public void testDefaultSizeForLargeIndex() {
        Column c = new Column("c", Value.STRING);
        c.setOriginalSQL("VARCHAR(" + 300 + ")");

        List<InlineIndexColumn> inlineIdxs = new ArrayList<>();
        inlineIdxs.add(new StringInlineIndexColumn(c, false));

        assertEquals(H2TreeIndexBase.IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT, H2TreeIndexBase.computeInlineSize(inlineIdxs, -1, -1));
    }
    
}
