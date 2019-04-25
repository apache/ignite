/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for {@link GridHandleTable}.
 */
public class GridHandleTableSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGrow() throws Exception {
        GridHandleTable table = new GridHandleTable(8, 2);

        for (int i = 0; i < 16; i++)
            assertEquals(-1, table.lookup(i));

        Object testObj = new Object();

        assertEquals(-1, table.lookup(testObj));

        assertEquals(16, table.lookup(testObj));

        int cnt = 0;

        for (Object obj : table.objects()) {
            if (obj == testObj)
                cnt++;
        }

        assertEquals(1, cnt);
    }
}
