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

package org.apache.ignite.internal.processors.cache.consistency;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class ImplicitTransactionalReadRepairTest extends AbstractFullSetReadRepairTest {
    /** Test parameters. */
    @Parameterized.Parameters(name = "getEntry={0}, async={1}")
    public static Collection parameters() {
        List<Object[]> res = new ArrayList<>();

        for (boolean raw : new boolean[] {false, true}) {
            for (boolean async : new boolean[] {false, true})
                res.add(new Object[] {raw, async});
        }

        return res;
    }

    /** GetEntry or just get. */
    @Parameterized.Parameter
    public boolean raw;

    /** Async. */
    @Parameterized.Parameter(1)
    public boolean async;

    /** {@inheritDoc} */
    @Override protected void testGet(Ignite initiator, Integer cnt, boolean all) throws Exception {
        prepareAndCheck(
            initiator,
            cnt,
            raw,
            async,
            (ReadRepairData data) -> {
                if (all)
                    GETALL_CHECK_AND_FIX.accept(data);
                else
                    GET_CHECK_AND_FIX.accept(data);

                ENSURE_FIXED.accept(data);
            });
    }

    /** {@inheritDoc} */
    @Override protected void testGetNull(Ignite initiator) throws Exception {
        prepareAndCheck(
            initiator,
            1,
            raw,
            async,
            (ReadRepairData data) -> {
                GET_NULL.accept(data); // first attempt.
                GET_NULL.accept(data); // second attempt (checks first attempt causes no changes/fixes/etc).
            });
    }

    /** {@inheritDoc} */
    @Override protected void testContains(Ignite initiator, Integer cnt, boolean all) throws Exception {
        prepareAndCheck(
            initiator,
            cnt,
            raw,
            async,
            (ReadRepairData data) -> {
                if (all)
                    CONTAINS_ALL_CHECK_AND_FIX.accept(data);
                else
                    CONTAINS_CHECK_AND_FIX.accept(data);

                ENSURE_FIXED.accept(data);
            });
    }
}
