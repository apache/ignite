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

    /** Assertions to ensure {@link CircularStringBuilder#insert(int, String)} method works */
    @Test
    public void testCSBInsert() {
        testSBInsert(5, "123456789", 4, "new", "56789");
        testSBInsert(5, "123456789", 5, "new", "w6789");
        testSBInsert(5, "123456789", 6, "new", "ew789");
        testSBInsert(5, "123456789", 7, "new", "new89");
        testSBInsert(5, "123456789", 8, "new", "8new9");
        testSBInsert(5, "123456789", 9, "new", "89new");
        testSBInsert(5, "1", 0, "new", "new1");
        testSBInsert(5, "12", 0, "new", "new12");
        testSBInsert(5, "123", 0, "new", "ew123");
        testSBInsert(5, "1234", 0, "new", "w1234");
        testSBInsert(2, "1", 0, "new", "w1");
        testSBInsert(2, "12", 0, "new", "12");
        testSBInsert(3, "12", 0, "new", "w12");
        testSBInsert(3, "12", 1, "new", "ew2");
    }

    /** Assertions to ensure {@link CircularStringBuilder#substring(int, int)} method works */
    @Test
    public void testSubstring() {
        CircularStringBuilder circularStrBuilder = new CircularStringBuilder(5);
        circularStrBuilder.append("abc");
        assertEquals("abc", circularStrBuilder.substring(0, 3));
        assertEquals("ab", circularStrBuilder.substring(0, 2));
        assertEquals("bc", circularStrBuilder.substring(1, 3));
        circularStrBuilder.append("de");
        assertEquals("abc", circularStrBuilder.substring(0, 3));
        assertEquals("ab", circularStrBuilder.substring(0, 2));
        assertEquals("bc", circularStrBuilder.substring(1, 3));
        assertEquals("abcde", circularStrBuilder.substring(0, 5));
        assertEquals("de", circularStrBuilder.substring(3, 5));
        assertEquals("abc", circularStrBuilder.substring(0, 3));
        circularStrBuilder.append("fg");
        assertEquals("cdefg", circularStrBuilder.substring(2, 7));
        assertEquals("cdef", circularStrBuilder.substring(2, 6));
        assertEquals("defg", circularStrBuilder.substring(3, 7));
        assertEquals("cdefg", circularStrBuilder.substring(0, 7));
        circularStrBuilder.append("hi");
        assertEquals("efg", circularStrBuilder.substring(0, 7));
        assertEquals("efghi", circularStrBuilder.substring(0, 9));
        circularStrBuilder.append("j");
        assertEquals("fghi", circularStrBuilder.substring(0, 9));
        assertEquals("fghij", circularStrBuilder.substring(0, 10));
        assertEquals("ghij", circularStrBuilder.substring(6, 10));
        assertEquals("", circularStrBuilder.substring(0, 5));
        assertEquals("f", circularStrBuilder.substring(0, 6));
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

    /**
     * Test ring buffer method {@link CircularStringBuilder#insert(int, String)}
     * @param capacity          ring buffer capacity
     * @param firstVal          value to append to buffer before test
     * @param offset            insert offset argument
     * @param insertSubstring   insert substring argument
     * @param expectedResult    expected ring buffer state
     *                          (to assert it equals to {@link CircularStringBuilder#toString()})
     */
    private void testSBInsert(int capacity,
                              String firstVal,
                              int offset,
                              String insertSubstring,
                              String expectedResult) {
        CircularStringBuilder csb = new CircularStringBuilder(capacity);
        csb.append(firstVal);
        csb.insert(offset, insertSubstring);
        assertEquals(expectedResult, csb.toString());
    }
}
