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
import org.junit.Assert;
import org.junit.Test;

/**
 * Test suite to ensure SBLimitedLength works by design
 */
@GridCommonTest(group = "Utils")
public class SBLimitedLengthSelfTest extends GridCommonAbstractTest {
    /** Ensure all append operations are working fine */
    @Test
    public void testAppend() {
        SBLimitedLength strBuilder = getStrBuilder(5);
        strBuilder.a(1);
        Assert.assertEquals("1", strBuilder.toString());
        strBuilder.a(2L);
        Assert.assertEquals("12", strBuilder.toString());
        strBuilder.a(3f);
        Assert.assertEquals("123.0", strBuilder.toString());
        strBuilder.a(4d);
        Assert.assertEquals("123.04.0", strBuilder.toString());
        strBuilder.a('5');
        Assert.assertEquals("123.04.05", strBuilder.toString());
        strBuilder.a(true);
        Assert.assertEquals("123.04.05true", strBuilder.toString());
        Object obj = "6";
        strBuilder.a(obj);
        Assert.assertEquals("123.04.05true6", strBuilder.toString());
        strBuilder.a("7");
        Assert.assertEquals("123.04.05true67", strBuilder.toString());
        strBuilder.a(new StringBuilder().append("8"));
        Assert.assertEquals("123.04.05true678", strBuilder.toString());
        CharSequence charSeq = "9";
        strBuilder.a(charSeq);
        Assert.assertEquals("123.04.05true6789", strBuilder.toString());
        strBuilder.a(charSeq, 0, 1);
        Assert.assertEquals("123.04.05true67899", strBuilder.toString());
    }

    /** */
    @Test
    public void testDoesNotThrowNPEOnHeadOverflow() {
        SBLimitedLength sbLimitedLength = new SBLimitedLength(256);
        sbLimitedLength.initLimit(new SBLengthLimit());
        sbLimitedLength.a("a".repeat(7999));
        sbLimitedLength.i(7000, "asd");
        sbLimitedLength.a("a".repeat(10));
        String result = sbLimitedLength.toString();
        Assert.assertNotNull(result);
        Assert.assertFalse(result.isEmpty());
        Assert.assertTrue(result.contains("asd"));
    }

    /**
     * Test that simulates the NPE scenario from handleRecursion.
     * When tail is null but overflowed() returns true, append operations should not throw NPE.
     */
    @Test
    public void testNPEProtectionWithNullTail() {
        SBLimitedLength sb = new SBLimitedLength(256);
        sb.initLimit(new SBLengthLimit());
        sb.a("a".repeat(8000));
        sb.i(0, "@0");
        sb.a("x");
    }

    /** Ensure toString works as expected */
    @Test
    public void testToString() {
        SBLimitedLength strBuilder = getStrBuilder(2);
        strBuilder.a("ab");
        Assert.assertEquals("ab", strBuilder.toString());
        strBuilder.a("cd");
        Assert.assertEquals("abcd", strBuilder.toString());
    }

    /**
     * Get {@link SBLimitedLength} instance with specific head and tail length
     * to simplify test cases
     * @param headLength Head length.
     */
    private SBLimitedLength getStrBuilder(int headLength) {
        SBLimitedLength sbLimitedLength = new SBLimitedLength(0);
        sbLimitedLength.initLimit(new SBLengthLimit() {
            @Override boolean overflowed(SBLimitedLength sb) {
                return sb.impl().length() > headLength;
            }
        });
        return sbLimitedLength;
    }
}



