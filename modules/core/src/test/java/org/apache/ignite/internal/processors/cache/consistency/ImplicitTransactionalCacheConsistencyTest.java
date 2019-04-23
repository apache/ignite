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

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;

/**
 *
 */
public class ImplicitTransactionalCacheConsistencyTest extends AbstractCacheConsistencyTest {
    /**
     *
     */
    @Test
    public void test() throws Exception {
        test(true);
        test(false);
    }

    /**
     *
     */
    private void test(boolean raw /*getEntry() or just get()*/) throws Exception {
        for (Ignite node : G.allGrids()) {
            testGet(node, raw);
            testGetAllVariations(node, raw);
            testGetNull(node, raw);
        }
    }

    /**
     *
     */
    private void testGet(Ignite initiator, boolean raw) throws Exception {
        prepareAndCheck(
            initiator,
            1,
            raw,
            (ConsistencyRecoveryData data) -> {
                GET_CHECK_AND_FIX.accept(data);
                ENSURE_FIXED.accept(data);
            });
    }

    /**
     *
     */
    private void testGetAllVariations(Ignite initiator, boolean raw) throws Exception {
        testGetAll(initiator, 1, raw); // 1 (all keys available at primary)
        testGetAll(initiator, 2, raw); // less than backups
        testGetAll(initiator, 3, raw); // equals to backups
        testGetAll(initiator, 4, raw); // equals to backups + primary
        testGetAll(initiator, 10, raw); // more than backups
    }

    /**
     *
     */
    private void testGetAll(Ignite initiator, Integer amount, boolean raw) throws Exception {
        prepareAndCheck(
            initiator,
            amount,
            raw,
            (ConsistencyRecoveryData data) -> {
                GETALL_CHECK_AND_FIX.accept(data);
                ENSURE_FIXED.accept(data);
            });
    }

    /**
     *
     */
    private void testGetNull(Ignite initiator, boolean raw) throws Exception {
        prepareAndCheck(
            initiator,
            1,
            raw,
            (ConsistencyRecoveryData data) -> {
                GET_NULL.accept(data); // first attempt.
                GET_NULL.accept(data); // second attempt (checks first attempt causes no changes/fixes/etc).
            });
    }
}
