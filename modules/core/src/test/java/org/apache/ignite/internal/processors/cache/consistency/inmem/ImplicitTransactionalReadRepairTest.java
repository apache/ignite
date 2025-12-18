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

package org.apache.ignite.internal.processors.cache.consistency.inmem;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.cache.consistency.AbstractFullSetReadRepairTest;
import org.apache.ignite.internal.processors.cache.consistency.ReadRepairDataGenerator.ReadRepairData;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 *
 */
@ParameterizedClass(name = "getEntry={0}, async={1}, misses={2}, nulls={3}, binary={4}")
@MethodSource("allTypesArgs")
public class ImplicitTransactionalReadRepairTest extends AbstractFullSetReadRepairTest {
    /** Test parameters. */
    public static Stream<Arguments> allTypesArgs() {
        return GridTestUtils.cartesianProduct(
                List.of(true, false),
                List.of(true, false),
                List.of(true, false),
                List.of(true, false),
                List.of(true, false)
        ).stream().map(Arguments::of);
    }

    /** GetEntry or just get. */
    @Parameter(0)
    public boolean raw;

    /** Async. */
    @Parameter(1)
    public boolean async;

    /** Misses. */
    @Parameter(2)
    public boolean misses;

    /** Nulls. */
    @Parameter(3)
    public boolean nulls;

    /** With binary. */
    @Parameter(4)
    public boolean binary;

    /** {@inheritDoc} */
    @Override protected void testGet(Ignite initiator, int cnt, boolean all) throws Exception {
        generateAndCheck(
            initiator,
            cnt,
            raw,
            async,
            misses,
            nulls,
            binary,
            (ReadRepairData rrd) -> repairIfRepairable.accept(rrd,
                () -> testReadRepair(rrd, all ? GETALL_CHECK_AND_REPAIR : GET_CHECK_AND_REPAIR, true)));
    }

    /** {@inheritDoc} */
    @Override protected void testContains(Ignite initiator, int cnt, boolean all) throws Exception {
        generateAndCheck(
            initiator,
            cnt,
            raw,
            async,
            misses,
            nulls,
            binary,
            (ReadRepairData rrd) -> repairIfRepairable.accept(rrd,
                () -> testReadRepair(rrd, all ? CONTAINS_ALL_CHECK_AND_REPAIR : CONTAINS_CHECK_AND_REPAIR, true)));
    }

    /** {@inheritDoc} */
    @Override protected void testGetNull(Ignite initiator, int cnt, boolean all) throws Exception {
        generateAndCheck(
            initiator,
            cnt,
            raw,
            async,
            misses,
            nulls,
            binary,
            (ReadRepairData rrd) -> testReadRepair(rrd, all ? GET_ALL_NULL : GET_NULL, false));
    }

    /**
     *
     */
    private void testReadRepair(ReadRepairData rrd, Consumer<ReadRepairData> readOp, boolean hit) {
        readOp.accept(rrd);

        if (hit)
            check(rrd, null, true); // Hit.
        else
            checkEventMissed(); // Miss.
    }
}
