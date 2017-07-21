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

package org.apache.ignite.internal.util.nio;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for {@link GridNioSessionMetaKey}.
 */
public class GridNioSessionMetaKeySelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testNextRandomKey() throws Exception {
        AtomicInteger keyGen = U.staticField(GridNioSessionMetaKey.class, "keyGen");

        int initVal = keyGen.get();

        int key = GridNioSessionMetaKey.nextUniqueKey();

        // Check key is greater than any real GridNioSessionMetaKey ordinal.
        assertTrue(key >= GridNioSessionMetaKey.values().length);

        // Check all valid and some invalid key values.
        for (int i = ++key; i < GridNioSessionMetaKey.MAX_KEYS_CNT + 10; i++) {
            if (i < GridNioSessionMetaKey.MAX_KEYS_CNT)
                assertEquals(i, GridNioSessionMetaKey.nextUniqueKey());
            else
                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return GridNioSessionMetaKey.nextUniqueKey();
                    }
                }, IllegalStateException.class, "Maximum count of NIO session keys in system is limited by");
        }

        keyGen.set(initVal);
    }
}