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

package org.apache.ignite.tools.checkstyle;

import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/** */
public class IgniteAbbrevationRuleTest {
    /** */
    @Test
    public void testVarToWords() {
        assertListEquals(
            Arrays.asList("XXX", "YYY", "ZZZ"),
            IgniteAbbrevationsRule.words("XXX_YYY_ZZZ")
        );

        assertListEquals(
            Arrays.asList("XXX"),
            IgniteAbbrevationsRule.words("XXX")
        );

        assertListEquals(
            Arrays.asList("i"),
            IgniteAbbrevationsRule.words("i")
        );

        assertListEquals(
            Arrays.asList("i1"),
            IgniteAbbrevationsRule.words("i1")
        );

        assertListEquals(
            Arrays.asList("my", "Name"),
            IgniteAbbrevationsRule.words("myName")
        );

        assertListEquals(
            Arrays.asList("my", "Name", "And", "Other"),
            IgniteAbbrevationsRule.words("myNameAndOther")
        );

        assertListEquals(
            Arrays.asList("my", "URL"),
            IgniteAbbrevationsRule.words("myURL")
        );

        assertListEquals(
            Arrays.asList("my", "URL1"),
            IgniteAbbrevationsRule.words("myURL1")
        );

        assertListEquals(
            Arrays.asList("a", "Name"),
            IgniteAbbrevationsRule.words("aName")
        );
    }

    /** */
    public void assertListEquals(List<String> expected, List<String> actual) {
        if (expected == actual)
            return;

        if (expected == null)
            throw new AssertionError("Expected null but got " + actual);

        if (actual == null)
            throw new AssertionError("Expected [" + String.join(", ", expected) + "] but got null");

        if (expected.size() != actual.size()) {
            throw new AssertionError("Size of list differs! " +
                "Expected [" + String.join(", ", expected) + "] but got [" + String.join(", ", actual) + ']');
        }

        for (int i = 0; i < expected.size(); i++)
            Assert.assertEquals("Expected same element at " + i + " index", expected.get(i), actual.get(i));
    }
}
