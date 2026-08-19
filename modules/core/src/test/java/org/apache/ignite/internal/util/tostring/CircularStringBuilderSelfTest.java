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

package org.apache.ignite.internal.util.tostring;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

/**
 *
 */
@GridCommonTest(group = "Utils")
public class CircularStringBuilderSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCSBPrimitive() throws Exception {
        CircularStringBuilder csb = new CircularStringBuilder(1);
        csb.append((String)null);
        assertEquals("l", csb.toString());
        csb.append('1');
        assertEquals("1", csb.toString());

        CircularStringBuilder csb2 = new CircularStringBuilder(1);
        csb2.append(1);
        assertEquals("1", csb2.toString());
    }

    /**
    * @throws Exception If failed.
    */
    @Test
    public void testCSBOverflow() throws Exception {
        testSB(3, "1234", 2, "234");
        testSB(4, "1234", 2, "1234");
        testSB(5, "1234", 2, "41234");
        testSB(6, "1234", 2, "341234");
        testSB(7, "1234", 2, "2341234");
        testSB(8, "1234", 2, "12341234");
    }

    /**
     * @param capacity Capacity.
     * @param pattern Pattern to add.
     * @param num How many times pattern should be added.
     * @param expected Expected string.
     */
    private void testSB(int capacity, String pattern, int num, String expected) {
        CircularStringBuilder csb = new CircularStringBuilder(capacity);

        for (int i = 0; i < num; i++)
            csb.append(pattern);

        assertEquals(expected, csb.toString());
    }
}
