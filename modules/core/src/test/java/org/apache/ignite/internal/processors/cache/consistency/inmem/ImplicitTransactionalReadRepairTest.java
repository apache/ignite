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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.processors.cache.consistency.AbstractFullSetReadRepairTest;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class ImplicitTransactionalReadRepairTest extends AbstractFullSetReadRepairTest {
    /** Test parameters. */
    @Parameterized.Parameters(name = "getEntry={0}, async={1}, misses={2}, nulls={3}")
    public static Collection parameters() {
        List<Object[]> res = new ArrayList<>();

        for (boolean raw : new boolean[] {false, true}) {
            for (boolean async : new boolean[] {false, true})
                for (boolean misses : new boolean[] {false, true})
                    for (boolean nulls : new boolean[] {false, true})
                        res.add(new Object[] {raw, async, misses, nulls});
        }

        return res;
    }

    /** GetEntry or just get. */
    @Parameterized.Parameter
    public boolean raw;

    /** Async. */
    @Parameterized.Parameter(1)
    public boolean async;

    /** Misses. */
    @Parameterized.Parameter(2)
    public boolean misses;

    /** Nulls. */
    @Parameterized.Parameter(3)
    public boolean nulls;

    /** {@inheritDoc} */
    @Override protected void testGet(Ignite initiator, Integer cnt, boolean all) throws Exception {
        prepareAndCheck(
            initiator,
            cnt,
            raw,
            async,
            misses,
            nulls,
            (ReadRepairData data) -> repairIfRepairable.accept(data,
                () -> testReadRepair(data, all ? GETALL_CHECK_AND_FIX : GET_CHECK_AND_FIX, true)));
    }

    /** {@inheritDoc} */
    @Override protected void testContains(Ignite initiator, Integer cnt, boolean all) throws Exception {
        prepareAndCheck(
            initiator,
            cnt,
            raw,
            async,
            misses,
            nulls,
            (ReadRepairData data) -> repairIfRepairable.accept(data,
                () -> testReadRepair(data, all ? CONTAINS_ALL_CHECK_AND_FIX : CONTAINS_CHECK_AND_FIX, true)));
    }

    /** {@inheritDoc} */
    @Override protected void testGetNull(Ignite initiator, Integer cnt, boolean all) throws Exception {
        prepareAndCheck(
            initiator,
            cnt,
            raw,
            async,
            misses,
            nulls,
            (ReadRepairData data) -> testReadRepair(data, all ? GET_ALL_NULL : GET_NULL, false));
    }

    /**
     *
     */
    private void testReadRepair(ReadRepairData data, Consumer<ReadRepairData> readOp, boolean hit) {
        readOp.accept(data);

        if (hit)
            check(data, null, true); // Hit.
        else
            checkEventMissed(); // Miss.
    }
}
