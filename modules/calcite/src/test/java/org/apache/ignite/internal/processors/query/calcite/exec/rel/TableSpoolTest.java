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

import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

import static java.lang.Math.min;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened")
@WithSystemProperty(key = "calcite.debug", value = "true")
public class TableSpoolTest extends AbstractExecutionTest {
    /**
     * @throws Exception If failed.
     */
    @Before
    @Override public void setup() throws Exception {
        nodesCnt = 1;
        super.setup();
    }

    /**
     *
     */
    @Test
    public void testTableSpool() throws Exception {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class, int.class);

        int inBufSize = U.field(AbstractNode.class, "IN_BUFFER_SIZE");

        int[] leftSizes = {1, inBufSize / 2 - 1, inBufSize / 2, inBufSize / 2 + 1, inBufSize, inBufSize + 1, inBufSize * 4};
        int[] rightSizes = {1, inBufSize / 2 - 1, inBufSize / 2, inBufSize / 2 + 1, inBufSize, inBufSize + 1, inBufSize * 4};
        int[] rightBufSizes = {1, inBufSize / 4, inBufSize};

        for (int rightBufSize : rightBufSizes) {
            for (int leftSize : leftSizes) {
                for (int rightSize : rightSizes) {
                    log.info("Check: rightBufSize=" + rightBufSize + ", leftSize=" + leftSize + ", rightSize=" + rightSize);

                    ScanNode<Object[]> left = new ScanNode<>(ctx, rowType, new TestTable(leftSize, rowType));
                    ScanNode<Object[]> right = new ScanNode<>(ctx, rowType, new TestTable(rightSize, rowType) {
                        boolean first = true;

                        @Override public @NotNull Iterator<Object[]> iterator() {
                            assertTrue("Rewind right", first);

                            first = false;
                            return super.iterator();
                        }
                    });

                    TableSpoolNode<Object[]> rightSpool = new TableSpoolNode<>(ctx, rowType);

                    rightSpool.register(Arrays.asList(right));

                    RelDataType joinRowType = TypeUtils.createRowType(
                        tf,
                        int.class, String.class, int.class,
                        int.class, String.class, int.class);

                    CorrelatedNestedLoopJoinNode<Object[]> join = new CorrelatedNestedLoopJoinNode<>(
                        ctx,
                        joinRowType,
                        r -> r[0].equals(r[3]),
                        ImmutableSet.of(new CorrelationId(0)));

                    GridTestUtils.setFieldValue(join, "rightInBufferSize", rightBufSize);

                    join.register(Arrays.asList(left, rightSpool));

                    RootNode<Object[]> root = new RootNode<>(ctx, joinRowType);
                    root.register(join);

                    int cnt = 0;
                    while (root.hasNext()) {
                        root.next();

                        cnt++;
                    }

                    assertEquals(
                        "Invalid result size. [left=" + leftSize + ", right=" + rightSize + ", results=" + cnt,
                        min(leftSize, rightSize),
                        cnt);
                }
            }
        }
    }
}
