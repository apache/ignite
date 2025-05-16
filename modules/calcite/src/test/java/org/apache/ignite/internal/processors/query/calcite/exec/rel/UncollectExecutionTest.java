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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
    @Test
    public void testCollectionSizes() {
        for (boolean withOrdinality : new boolean[] {false, true}) {
            int rows = 100;

            int[] sizes = {1, IN_BUFFER_SIZE / 2 - 1, IN_BUFFER_SIZE / 2, IN_BUFFER_SIZE / 2 + 1, IN_BUFFER_SIZE,
                IN_BUFFER_SIZE + 1, IN_BUFFER_SIZE * 4};

            for (int size : sizes) {
                log.info("Check: size=" + size + ", withOrdinality=" + withOrdinality);

                Function<Integer, Object> func = row -> IntStream.range(0, size).boxed().collect(Collectors.toList());

                RootRewindable<Object[]> root = createNodes(withOrdinality, new TestTable(rows, 1, func));

                long[] colsSum = new long[withOrdinality ? 2 : 1];
                int cnt = 0;

                while (root.hasNext()) {
                    cnt++;
                    Object[] row = root.next();

                    for (int i = 0; i < row.length; i++)
                        colsSum[i] += (int)row[i];
                }

                assertEquals(size * rows, cnt);

                // Expected value of unnested column = rows * sum(0 .. size - 1)
                assertEquals("Unexpected sum: size=" + size, (long)size * (size - 1) / 2 * rows, colsSum[0]);

                if (withOrdinality) // Expected value of ordinality column = rows * sum(1 .. size)
                    assertEquals("Unexpected ordinality: size=" + size, (long)size * (size + 1) / 2 * rows, colsSum[1]);

                root.closeRewindableRoot();
            }
        }
    }

    /** */
    @Test
    public void testExactResult() {
        checkExactResult(
            false,
            F.asList(
                row(F.asList(1, 2)),
                row(F.asList(1))
            ),
            F.asList(
                row(1), row(2), row(1)
            )
        );

        checkExactResult(
            true,
            F.asList(
                row(F.asList(1, 2)),
                row(F.asList(3, 4, 5)),
                row(F.asList(6, 7))
            ),
            F.asList(
                row(1, 1), row(2, 2),
                row(3, 1), row(4, 2), row(5, 3),
                row(6, 1), row(7, 2)
            )
        );

        checkExactResult(
            true,
            F.asList(
                row(F.asList(1, 2)),
                row(Collections.emptyList())
            ),
            F.asList(
                row(1, 1), row(2, 2)
            )
        );
    }

    /** */
    private void checkExactResult(boolean withOrdinality, List<Object[]> dataSrc, List<Object[]> expRes) {
        RootRewindable<Object[]> root = createNodes(withOrdinality, dataSrc);

        List<Object[]> res = new ArrayList<>();

        while (root.hasNext())
            res.add(root.next());

        assertEquals(expRes.size(), res.size());

        for (int i = 0; i < expRes.size(); i++)
            assertEqualsArraysAware(expRes.get(i), res.get(i));

        root.closeRewindableRoot();
    }

    /** */
    private RootRewindable<Object[]> createNodes(boolean withOrdinality, Iterable<Object[]> dataSrc) {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();

        RelDataType[] inCols = new RelDataType[] {tf.createArrayType(tf.createSqlType(SqlTypeName.INTEGER), -1)};
        RelDataType[] outCols = new RelDataType[withOrdinality ? 2 : 1];

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
