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

package org.apache.ignite.internal.processors.query.calcite.exec.rel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 * Test for UNCOLLECT node execution.
 */
public class UncollectExecutionTest extends AbstractExecutionTest {
    /** */
    @SuppressWarnings("unchecked")
    @Test
    public void testSizes() {
        for (boolean withOrdinality : new boolean[] {false, true}) {
            for (int colCnt : new int[] {1, 2, 3}) {
                int[] sizes = {1, inBufSize / 2 - 1, inBufSize / 2, inBufSize / 2 + 1, inBufSize, inBufSize + 1, inBufSize * 4};

                for (int size : sizes) {
                    log.info("Check: size=" + size + ", colCnt=" + colCnt + ", withOrdinality=" + withOrdinality);

                    Function<Integer, Object>[] funcs = new Function[colCnt];

                    for (int i = 0; i < colCnt; i++) {
                        int mul = 2 << i;
                        funcs[i] = r -> F.asList(mul * r, mul * r + 1);
                    }

                    RootRewindable<Object[]> root = createNodes(colCnt, withOrdinality, new TestTable(size, colCnt, funcs));

                    long[] colsSum = new long[withOrdinality ? colCnt + 1 : colCnt];
                    int cnt = 0;

                    while (root.hasNext()) {
                        cnt++;
                        Object[] row = root.next();

                        for (int i = 0; i < row.length; i++)
                            colsSum[i] += (int)row[i];
                    }

                    assertEquals(size * (1 << colCnt), cnt);

                    for (int i = 0; i < colCnt; i++) {
                        long expSum = (size * (size - 1L) * (2L << i) + size) * (1L << (colCnt - 1));
                        assertEquals("Unexpected sum: size=" + size + ", colCnt=" + colCnt +
                            ", withOrdinality=" + withOrdinality + ", i=" + i, expSum, colsSum[i]);
                    }

                    long ordSum[] = {1, 3, 10, 36}; // sum(1 .. (1 << colCnt))

                    if (withOrdinality)
                        assertEquals(size * ordSum[colCnt], colsSum[colCnt]);

                    root.closeRewindableRoot();
                }
            }
        }
    }

    /** */
    @Test
    public void testExactResult() {
        checkExactResult(
            2,
            false,
            F.asList(
                row(F.asList(1, 2), F.asList(3, 4)),
                row(F.asList(1), F.asList(2, 3))
            ),
            F.asList(
                row(1, 3), row(1, 4),
                row(2, 3), row(2, 4),
                row(1, 2), row(1, 3)
            )
        );

        checkExactResult(
            2,
            true,
            F.asList(
                row(F.asList(1, 2), F.asList(3, 4)),
                row(F.asList(1), F.asList(2, 3))
            ),
            F.asList(
                row(1, 3, 1), row(1, 4, 2),
                row(2, 3, 3), row(2, 4, 4),
                row(1, 2, 1), row(1, 3, 2)
            )
        );

        checkExactResult(
            2,
            true,
            F.asList(
                row(F.asList(1, 2), Collections.emptyList()),
                row(Collections.emptyList(), F.asList(1, 2))
            ),
            Collections.emptyList()
        );

        checkExactResult(
            3,
            true,
            F.asList(
                row(F.asList(1, 2), F.asList(3, 4), F.asList(5, 6, 7)),
                row(F.asList(1), F.asList(2, 3), F.asList(4, 5, 6))
            ),
            F.asList(
                row(1, 3, 5, 1), row(1, 3, 6, 2), row(1, 3, 7, 3),
                row(1, 4, 5, 4), row(1, 4, 6, 5), row(1, 4, 7, 6),
                row(2, 3, 5, 7), row(2, 3, 6, 8), row(2, 3, 7, 9),
                row(2, 4, 5, 10), row(2, 4, 6, 11), row(2, 4, 7, 12),
                row(1, 2, 4, 1), row(1, 2, 5, 2), row(1, 2, 6, 3),
                row(1, 3, 4, 4), row(1, 3, 5, 5), row(1, 3, 6, 6)
            )
        );
    }

    /** */
    private void checkExactResult(int colCnt, boolean withOrdinality, List<Object[]> dataSrc, List<Object[]> expRes) {
        RootRewindable<Object[]> root = createNodes(colCnt, withOrdinality, dataSrc);

        List<Object[]> res = new ArrayList<>();

        while (root.hasNext())
            res.add(root.next());

        assertEquals(expRes.size(), res.size());

        for (int i = 0; i < expRes.size(); i++)
            assertEqualsArraysAware(expRes.get(i), res.get(i));

        root.closeRewindableRoot();
    }

    /** */
    private RootRewindable<Object[]> createNodes(int colCnt, boolean withOrdinality, Iterable<Object[]> dataSrc) {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();

        RelDataType[] inCols = new RelDataType[colCnt];
        RelDataType[] outCols = new RelDataType[withOrdinality ? colCnt + 1 : colCnt];

        Arrays.fill(inCols, tf.createArrayType(tf.createSqlType(SqlTypeName.INTEGER), -1));
        Arrays.fill(outCols, tf.createSqlType(SqlTypeName.INTEGER));

        RelDataType inType = TypeUtils.createRowType(tf, inCols);
        RelDataType outType = TypeUtils.createRowType(tf, outCols);

        ScanNode<Object[]> scan = new ScanNode<>(ctx, inType, dataSrc);

        UncollectNode<Object[]> uncollect = new UncollectNode<>(ctx, inType, outType, withOrdinality);
        uncollect.register(scan);

        RootRewindable<Object[]> root = new RootRewindable<>(ctx, outType);
        root.register(uncollect);

        return root;
    }
}
