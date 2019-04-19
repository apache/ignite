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

package org.apache.ignite.internal;

import java.util.Calendar;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 */
public class IgniteVersionUtilsSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIgniteCopyrights() throws Exception {
        final String COPYRIGHT = String.valueOf(Calendar.getInstance().get(Calendar.YEAR)) + " Copyright(C) Apache Software Foundation";

        assertNotNull(IgniteVersionUtils.COPYRIGHT);

        assertTrue(COPYRIGHT.equals(IgniteVersionUtils.COPYRIGHT));
    }
}
