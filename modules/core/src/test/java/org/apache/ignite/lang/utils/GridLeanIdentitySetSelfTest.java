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

package org.apache.ignite.lang.utils;

import java.util.Set;
import org.apache.ignite.internal.util.GridLeanIdentitySet;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Tests for {@link org.apache.ignite.internal.util.GridLeanMap}.
 */
@GridCommonTest(group = "Lang")
public class GridLeanIdentitySetSelfTest extends GridCommonAbstractTest {
    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testAddSizeContainsClear() throws Exception {
        Set<Integer> set = new GridLeanIdentitySet<>();

        assert set.isEmpty();

        for (int i = 0; i < 100; i++) {
            assertEquals(i, set.size());

            for (int j = 0; j < 100; j++) {
                if (j < i) {
                    assert set.contains(Integer.valueOf(j));

                    assert !set.add(j);
                }
                else
                    assert !set.contains(Integer.valueOf(j));
            }

            assert set.add(i);

            assert !set.isEmpty();
        }

        set.clear();

        assert set.isEmpty();
    }
}