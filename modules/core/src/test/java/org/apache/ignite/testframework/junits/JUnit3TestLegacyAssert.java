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

package org.apache.ignite.testframework.junits;

/**
 * Supports compatibility with old tests that expect inherited assertions.
 */
class JUnit3TestLegacyAssert /*extends  junit.framework.Assert*/ {
    /**
     * Asserts that a cond is true. If it isn't it throws
     * an AssertionFailedError with the given msg.
     */
    protected static void assertTrue(String msg, boolean cond) {
        // todo keep and deledate
    }

    /**
     * Asserts that a cond is true. If it isn't it throws
     * an AssertionFailedError.
     */
    protected static void assertTrue(boolean cond) {
        // todo keep and deledate
    }

    /**
     * Asserts that a cond is false. If it isn't it throws
     * an AssertionFailedError with the given msg.
     */
    protected static void assertFalse(String msg, boolean cond) {
        // todo keep and deledate
    }

    /**
     * Asserts that a cond is false. If it isn't it throws
     * an AssertionFailedError.
     */
    protected static void assertFalse(boolean cond) {
        // todo keep and deledate
    }

    /**
     * Asserts that two objs are equal. If they are not
     * an AssertionFailedError is thrown with the given msg.
     */
    protected static void assertEquals(String msg, Object exp, Object actual) {
        // todo keep and deledate
    }

    /**
     * Asserts that two objs are equal. If they are not
     * an AssertionFailedError is thrown.
     */
    protected static void assertEquals(Object exp, Object actual) {
        // todo keep and deledate
    }

    /**
     * Asserts that two Strings are equal.
     */
    protected static void assertEquals(String msg, String exp, String actual) {
        // todo keep and deledate
    }

    /**
     * Asserts that two Strings are equal.
     */
    protected static void assertEquals(String exp, String actual) {
        // todo keep and deledate
    }

    /**
     * Asserts that two doubles are equal concerning a delta. If the exp
     * value is infinity then the delta value is ignored.
     */
    protected static void assertEquals(double exp, double actual, double delta) {
        // todo keep and deledate
    }

    /**
     * Asserts that two floats are equal concerning a delta. If the exp
     * value is infinity then the delta value is ignored.
     */
    protected static void assertEquals(float exp, float actual, float delta) {
        // todo keep and deledate
    }

    /**
     * Asserts that two longs are equal. If they are not
     * an AssertionFailedError is thrown with the given msg.
     */
    protected static void assertEquals(String msg, long exp, long actual) {
        // todo keep and deledate
    }

    /**
     * Asserts that two longs are equal.
     */
    protected static void assertEquals(long exp, long actual) {
        // todo keep and deledate
    }

    /**
     * Asserts that two booleans are equal. If they are not
     * an AssertionFailedError is thrown with the given msg.
     */
    protected static void assertEquals(String msg, boolean exp, boolean actual) {
        // todo remove and edit
    }

    /**
     * Asserts that two booleans are equal.
     */
    protected static void assertEquals(boolean exp, boolean actual) {
        // todo keep and deledate
    }

    /**
     * Asserts that two bytes are equal. If they are not
     * an AssertionFailedError is thrown with the given msg.
     */
    protected static void assertEquals(String msg, byte exp, byte actual) {
        // todo remove and edit
    }

    /**
     * Asserts that two bytes are equal.
     */
    protected static void assertEquals(byte exp, byte actual) {
        // todo keep and deledate
    }

    /**
     * Asserts that two chars are equal.
     */
    protected static void assertEquals(char exp, char actual) {
        // todo re-check whether to deledate
    }

    /**
     * Asserts that two shorts are equal.
     */
    protected static void assertEquals(short exp, short actual) {
        // todo re-check whether to deledate
    }

    /**
     * Asserts that two ints are equal. If they are not
     * an AssertionFailedError is thrown with the given msg.
     */
    protected static void assertEquals(String msg, int exp, int actual) {
        // todo keep and deledate
    }

    /**
     * Asserts that two ints are equal.
     */
    protected static void assertEquals(int exp, int actual) {
        // todo keep and deledate
    }

    /**
     * Asserts that an obj isn't null.
     */
    protected static void assertNotNull(Object obj) {
        // todo keep and deledate
    }

    /**
     * Asserts that an obj isn't null. If it is
     * an AssertionFailedError is thrown with the given msg.
     */
    protected static void assertNotNull(String msg, Object obj) {
        // todo keep and deledate
    }

    /**
     * Asserts that an obj is null. If it isn't an {@link AssertionError} is
     * thrown.
     * msg contains: exp: <null> but was: obj
     *
     * @param obj Object to check or <code>null</code>
     */
    protected static void assertNull(Object obj) {
        // todo keep and deledate
    }

    /**
     * Asserts that an obj is null.  If it is not
     * an AssertionFailedError is thrown with the given msg.
     */
    protected static void assertNull(String msg, Object obj) {
        // todo keep and deledate
    }

    /**
     * Asserts that an obj is null.  If it is not
     * an AssertionFailedError is thrown with the given msg.
     */
    protected static void fail(String msg) {
        // todo remove after merge of IGNITE-10178
    }

    /**
     * Asserts that an obj is null.  If it is not
     * an AssertionFailedError is thrown with the given msg.
     */
    protected static void fail() {
        // todo re-check whether to deledate
    }

    /**
     * Asserts that two objects do not refer to the same object. If they do
     * refer to the same object an AssertionFailedError is thrown.
     */
    protected static void assertSame(Object exp, Object actual) {
        // todo re-check whether to deledate
    }

    /**
     * Asserts that two objects do not refer to the same object. If they do
     * refer to the same object an AssertionFailedError is thrown.
     */
    protected static void assertNotSame(Object exp, Object actual) {
        // todo re-check whether to deledate
    }
}
