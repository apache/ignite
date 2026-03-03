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
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.TableRowIterable;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 * Execution test for ScanTableRowNode.
 */
public class ScanTableRowExecutionTest extends AbstractExecutionTest {
    /** */
    @Test
    public void testScanTableRow() {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class, Boolean.class);

        List<Object[]> data = F.asList(
            new Object[] {0, "0", true},
            new Object[] {1, "1", false},
            new Object[] {2, "2", true},
            new Object[] {3, "3", false},
            new Object[] {4, "4", true}
        );

        TableRowIterable<Object[], Object[]> it = new TableRowIterable<>() {
            @Override public Iterator<Object[]> tableRowIterator() {
                return data.iterator();
            }

            @Override public Object[] enrichRow(Object[] tableRow, Object[] nodeRow, int[] fieldColMapping) {
                for (int i = 0; i < fieldColMapping.length; i++) {
                    if (fieldColMapping[i] >= 0)
                        nodeRow[i] = tableRow[fieldColMapping[i]];
                }

                // Due to test invariant we can't return row enriched with FALSE in the field 2.
                assertNotSame(Boolean.FALSE, nodeRow[2]);

                return nodeRow;
            }

            @Override public Iterator<Object[]> iterator() {
                throw new AssertionError("Unexpected call");
            }
        };

        ScanTableRowNode<Object[], Object[]> scan = new ScanTableRowNode<>(
            "test",
            ctx,
            rowType, // Output row type.
            rowType, // Input row type.
            it, // Iterator.
            r -> ((int)r[0] & 1) == 0, // Filter.
            r -> new Object[] {((int)r[0]) * 2, r[1], r[2]}, // Project.
            new int[] {0, -1, -1}, // Filter columns mapping.
            new int[] {-1, 1, 2} // Other columns mapping.
        );

        RootNode<Object[]> root = new RootNode<>(ctx, rowType);
        root.register(scan);

        List<Object[]> res = new ArrayList<>();

        while (root.hasNext())
            res.add(root.next());

        List<Object[]> exp = F.asList(
            new Object[] {0, "0", true},
            new Object[] {4, "2", true},
            new Object[] {8, "4", true}
        );

        assertEquals(exp.size(), res.size());

        for (int i = 0; i < exp.size(); i++)
            assertEqualsArraysAware(exp.get(i), res.get(i));
    }
}
