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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.AbstractExecutionTest;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 *
 */
public class RuntimeTreeIndexTest extends AbstractExecutionTest {
    /** */
    private static final int ROWS = 10_000;

    /** */
    @Test
    public void test() throws Exception {
        ExecutionContext<Object[]> ctx = executionContext(F.first(nodes()), UUID.randomUUID(), 0);
        IgniteTypeFactory tf = ctx.getTypeFactory();

        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class, int.class);

        RuntimeTreeIndex<Object[]> idx0 = generate(rowType, Arrays.asList(0));

        System.out.println();
    }

    /** */
    private RuntimeTreeIndex<Object[]> generate(RelDataType rowType, final List<Integer> idxCols, double... selectivity) {
        RuntimeTreeIndex<Object[]> idx = new RuntimeTreeIndex<>(null, (o1, o2) -> {
            for (int colIdx : idxCols) {
                int res = ((Comparable)o1[colIdx]).compareTo(o2[colIdx]);

                if (res != 0)
                    return res;
            }

            return 0;
        });

        BitSet rowIds = new BitSet(ROWS);

        // First random fill
        for (int i = 0; i < ROWS; ++i) {
            int rowId = ThreadLocalRandom.current().nextInt(ROWS);

            if (!rowIds.get(rowId)) {
                idx.push(generateRow(rowId, rowType, selectivity));
                rowIds.set(rowId);
            }
        }

        for (int i = 0; i < ROWS; ++i) {
            if (!rowIds.get(i))
                idx.push(generateRow(i, rowType, selectivity));
        }

        return idx;
    }

    /** */
    private Object[] generateRow(int rowId, RelDataType rowType, double... selectivity) {
        Object[] row = new Object[rowType.getFieldCount()];

        for (int i = 0; i < rowType.getFieldCount(); ++i)
            row[i] = generateValue(rowId, rowType.getFieldList().get(i), selectivity.length > i ? selectivity[i] : 1.0d);

        return row;
    }

    /** */
    private Object generateValue(int rowId, RelDataTypeField field, double selectivity) {
        long mod = rowId % (int)(ROWS * selectivity);
        long baseDate = 1_000_000L;

        switch (field.getType().getSqlTypeName().getFamily()) {
            case NUMERIC:
                return mod;

            case CHARACTER:
                return "val " + mod;

            case DATE:
                return new Date(baseDate + mod * 10_000);

            case TIME:
                return new Time(mod);

            case TIMESTAMP:
                return new Timestamp(baseDate + mod);

            default:
                assert false : "Not supported type for test: " + field.getType();
                return null;
        }
    }
}
