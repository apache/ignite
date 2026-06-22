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
        SBLimitedLength strBuilder = getStrBuilder(5, 50);
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
        strBuilder.a(new char[]{'a'});
        Assert.assertEquals("123.04.05true67899a", strBuilder.toString());
        strBuilder.a(new char[]{'b', 'c', 'd'}, 0, 2);
        Assert.assertEquals("123.04.05true67899abc", strBuilder.toString());
    }

    /** Ensure all insert operations are working fine */
    @Test
    public void testInsert() {
        SBLimitedLength strBuilder = getStrBuilder(5, 50);
        strBuilder.i(0, 1);
        Assert.assertEquals("1", strBuilder.toString());
        strBuilder.i(0, 2L);
        Assert.assertEquals("21", strBuilder.toString());
        strBuilder.i(0, 3f);
        Assert.assertEquals("3.021", strBuilder.toString());
        strBuilder.i(0, 4d);
        Assert.assertEquals("4.03.021", strBuilder.toString());
        strBuilder.i(0, true);
        Assert.assertEquals("true4.03.021", strBuilder.toString());
        strBuilder.i(0, '5');
        Assert.assertEquals("5true4.03.021", strBuilder.toString());
        strBuilder.i(1, "6");
        Assert.assertEquals("56true4.03.021", strBuilder.toString());
        strBuilder.i(2, new char[] {'a', 'b', 'c', 'd'});
        Assert.assertEquals("56abcdtrue4.03.021", strBuilder.toString());
        strBuilder.i(5, new char[] {'e', 'f', 'g', 'i'}, 0, 3);
        Assert.assertEquals("56abcefgdtrue4.03.021", strBuilder.toString());
        Object obj = "h";
        strBuilder.i(6, obj);
        Assert.assertEquals("56abcehfgdtrue4.03.021", strBuilder.toString());
        CharSequence charSeq = "ijk";
        strBuilder.i(7, charSeq);
        Assert.assertEquals("56abcehijkfgdtrue4.03.021", strBuilder.toString());
        strBuilder.i(8, charSeq, 0, 2);
        Assert.assertEquals("56abcehiijjkfgdtrue4.03.021", strBuilder.toString());
    }

    /** Ensure toString works as expected */
    @Test
    public void testToString() {
        SBLimitedLength strBuilder = getStrBuilder(2, 2);
        strBuilder.a("ab");
        Assert.assertEquals("ab", strBuilder.toString());
        strBuilder.a("cd");
        Assert.assertEquals("abcd", strBuilder.toString());
        strBuilder.a("ef");
        Assert.assertEquals("ab... and 4 skipped ...ef", strBuilder.toString());
    }

    /** Ensure all operations that could possibly reduce length are prohibited */
    @Test
    public void testLengthReduceOperationsAreProhibited() {
        SBLimitedLength strBuilder = getStrBuilder(2, 2);
        assertThrows(UnsupportedOperationException.class, () -> strBuilder.d(0));
        assertThrows(UnsupportedOperationException.class, () -> strBuilder.d(0, 0));
        assertThrows(UnsupportedOperationException.class, () -> strBuilder.r(0, 0, "asd"));
        assertThrows(UnsupportedOperationException.class, () -> strBuilder.setLength(0));
    }

    /**
     * Assert {@link Runnable#run()} will throw specified exception
     * @param expectedExceptionClass Expected exception class.
     * @param runnable Runnable.
     */
    private void assertThrows(Class<? extends Throwable> expectedExceptionClass, Runnable runnable) {
        boolean eIsSpotted = false;
        try {
            runnable.run();
        }
        catch (Throwable throwable) {
            if (expectedExceptionClass.isAssignableFrom(throwable.getClass()))
                eIsSpotted = true;
        }
        finally {
            Assert.assertTrue(eIsSpotted);
        }
    }

    /**
     * Get {@link SBLimitedLength} instance with specific head and tail length
     * to simplify test cases
     * @param headLength Head length.
     * @param tailLength Tail length.
     */
    private SBLimitedLength getStrBuilder(int headLength, int tailLength) {
        SBLimitedLength sbLimitedLength = new SBLimitedLength(0);
        sbLimitedLength.initLimit(new SBLengthLimit() {

            @Override int getHeadLengthLimit() {
                return headLength;
            }

            @Override int getTailLengthLimit() {
                return tailLength;
            }
        });
        return sbLimitedLength;
    }
}



