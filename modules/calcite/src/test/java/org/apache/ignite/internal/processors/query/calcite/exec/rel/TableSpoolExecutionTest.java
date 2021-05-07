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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
@SuppressWarnings("TypeMayBeWeakened")
@WithSystemProperty(key = "calcite.debug", value = "true")
public class TableSpoolExecutionTest extends AbstractExecutionTest {
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

        int[] sizes = {1, inBufSize / 2 - 1, inBufSize / 2, inBufSize / 2 + 1, inBufSize, inBufSize + 1, inBufSize * 4};
//        int[] sizes = {inBufSize * 4};
        int rewindCnts = 32;

        for (int size : sizes) {
            log.info("Check: size=" + size);

            ScanNode<Object[]> right = new ScanNode<>(ctx, rowType, new TestTable(size, rowType) {
                boolean first = true;

                @Override public @NotNull Iterator<Object[]> iterator() {
                    assertTrue("Rewind table", first);

                    first = false;
                    return super.iterator();
                }
            });

            TableSpoolNode<Object[]> spool = new TableSpoolNode<>(ctx, rowType, true);

            spool.register(Arrays.asList(right));

            RootRewindable<Object[]> root = new RootRewindable<>(ctx, rowType);
            root.register(spool);

            for (int i = 0; i < rewindCnts; ++i) {
                int cnt = 0;

                while (root.hasNext()) {
                    root.next();

                    cnt++;
                }

                assertEquals(
                    "Invalid result size",
                    size,
                    cnt);

                root.rewind();
            }
        }
    }
}
