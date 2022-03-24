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
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.agg.AggregateType;

/**
 * Test execution of INTERSECT operator.
 */
public class IntersectExecutionTest extends AbstractSetOpExecutionTest {
    /** {@inheritDoc} */
    @Override protected AbstractSetOpNode<Object[]> setOpNodeFactory(ExecutionContext<Object[]> ctx,
        RelDataType rowType, AggregateType type, boolean all, int inputsCnt) {
        return new IntersectNode<>(ctx, rowType, type, all, rowFactory(), inputsCnt);
    }

    /** {@inheritDoc} */
    @Override protected void checkSetOp(boolean single, boolean all) {
        List<Object[]> ds1 = Arrays.asList(
            row("Igor", 1),
            row("Roman", 1),
            row("Igor", 1),
            row("Roman", 2),
            row("Igor", 1),
            row("Igor", 1),
            row("Igor", 2)
        );

        List<Object[]> ds2 = Arrays.asList(
            row("Igor", 1),
            row("Roman", 1),
            row("Igor", 1),
            row("Igor", 1),
            row("Alexey", 1)
        );

        List<Object[]> ds3 = Arrays.asList(
            row("Igor", 1),
            row("Roman", 1),
            row("Igor", 1),
            row("Roman", 2),
            row("Alexey", 2)
        );

        List<Object[]> expectedResult;

        if (all) {
            expectedResult = Arrays.asList(
                row("Igor", 1),
                row("Igor", 1),
                row("Roman", 1)
            );
        }
        else {
            expectedResult = Arrays.asList(
                row("Igor", 1),
                row("Roman", 1)
            );
        }

        checkSetOp(single, all, Arrays.asList(ds1, ds2, ds3), expectedResult);
    }
}
