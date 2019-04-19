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

package org.apache.ignite.lang.utils;

import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashMap;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for {@link GridBoundedConcurrentLinkedHashMap}.
 */
@RunWith(JUnit4.class)
public class GridBoundedConcurrentLinkedHashMapSelfTest extends GridCommonAbstractTest {
    /** Bound. */
    private static final int MAX = 3;

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBound() throws Exception {
        Map<Integer, Integer> map = new GridBoundedConcurrentLinkedHashMap<>(MAX);

        for  (int i = 1; i <= 10; i++) {
            map.put(i, i);

            if (i <= MAX)
                assert map.size() == i;
            else
                assert map.size() == MAX;
        }

        assert map.size() == MAX;

        Iterator<Integer> it = map.values().iterator();

        assert it.next() == 8;
        assert it.next() == 9;
        assert it.next() == 10;
    }
}
