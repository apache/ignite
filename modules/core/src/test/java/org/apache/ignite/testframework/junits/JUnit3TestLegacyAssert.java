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

import org.junit.Assert;

/**
 * Supports compatibility with legacy tests that expect inherited assertions by delegating these to respective methods
 * in {@link Assert} in org.junit package.
 * Deprecation notice: instead of invoking inherited methods, directly refer to respective static methods of
 * {@link Assert} class.
 */
class JUnit3TestLegacyAssert {
    /** See class javadocs. */
    protected static void assertTrue(String msg, boolean cond) {
        Assert.assertTrue(msg, cond);
    }

    /** See class javadocs. */
    protected static void assertTrue(boolean cond) {
        Assert.assertTrue(cond);
    }

    /** See class javadocs. */
    protected static void assertFalse(String msg, boolean cond) {
        Assert.assertFalse(msg, cond);
    }

    /** See class javadocs. */
    protected static void assertFalse(boolean cond) {
        Assert.assertFalse(cond);
    }

    /** See class javadocs. */
    protected static void assertEquals(String msg, Object exp, Object actual) {
        Assert.assertEquals(msg, exp, actual);
    }

    /** See class javadocs. */
    protected static void assertEquals(Object exp, Object actual) {
        Assert.assertEquals(exp, actual);
    }

    /** See class javadocs. */
    protected static void assertEquals(String msg, String exp, String actual) {
        Assert.assertEquals(msg, exp, actual);
    }

    /** See class javadocs. */
    protected static void assertEquals(String exp, String actual) {
        Assert.assertEquals(exp, actual);
    }

    /** See class javadocs. */
    protected static void assertEquals(String msg, long exp, long actual) {
        Assert.assertEquals(msg, exp, actual);
    }

    /** See class javadocs. */
    protected static void assertEquals(long exp, long actual) {
        Assert.assertEquals(exp, actual);
    }

    /** See class javadocs. */
    protected static void assertEquals(boolean exp, boolean actual) {
        Assert.assertEquals(exp, actual);
    }

    /** See class javadocs. */
    protected static void assertEquals(String msg, int exp, int actual) {
        Assert.assertEquals(msg, exp, actual);
    }

    /** See class javadocs. */
    protected static void assertEquals(int exp, int actual) {
        Assert.assertEquals(exp, actual);
    }

    /** See class javadocs. */
    protected static void assertNotNull(Object obj) {
        Assert.assertNotNull(obj);
    }

    /** See class javadocs. */
    protected static void assertNotNull(String msg, Object obj) {
        Assert.assertNotNull(msg, obj);
    }

    /** See class javadocs. */
    protected static void assertNull(Object obj) {
        Assert.assertNull(obj);
    }

    /** See class javadocs. */
    protected static void assertNull(String msg, Object obj) {
        Assert.assertNull(msg, obj);
    }

    /** See class javadocs. */
    protected static void fail(String msg) {
        Assert.fail(msg);
    }

    /** See class javadocs. */
    protected static void fail() {
        Assert.fail();
    }

    /** See class javadocs. */
    protected static void assertSame(Object exp, Object actual) {
        Assert.assertSame(exp, actual);
    }

    /** See class javadocs. */
    protected static void assertNotSame(Object unexpected, Object actual) {
        Assert.assertNotSame(unexpected, actual);
    }

    /** See class javadocs. */
    protected static void assertEquals(String msg, double exp, double actual) {
        Assert.assertEquals(msg, exp, actual, 0);
    }

    /** See class javadocs. */
    protected static void assertEquals(double exp, Double actual) {
        Assert.assertEquals(exp, actual, 0);
    }

    /** See class javadocs. */
    protected static void assertEquals(Double exp, double actual) {
        Assert.assertEquals(exp, actual, 0);
    }

    /** See class javadocs. */
    protected static void assertEquals(double exp, double actual) {
        Assert.assertEquals(exp, actual, 0);
    }

    /** See class javadocs. */
    protected static void assertEquals(float exp, float actual) {
        Assert.assertEquals(exp, actual, 0);
    }

    /** See class javadocs. */
    protected static void assertEquals(float exp, Float actual) {
        Assert.assertEquals(exp, actual, 0);
    }

    /** See class javadocs. */
    protected static void assertEquals(String msg, double exp, double actual, double delta) {
        Assert.assertEquals(msg, exp, actual, delta);
    }

    /** See class javadocs. */
    protected static void assertEquals(double exp, double actual, double delta) {
        Assert.assertEquals(exp, actual, delta);
    }

    /** See class javadocs. */
    protected static void assertEquals(float exp, float actual, float delta) {
        Assert.assertEquals(exp, actual, delta);
    }

    /** See class javadocs. */
    protected static void assertEquals(String msg, boolean exp, boolean actual) {
        Assert.assertEquals(msg, exp, actual);
    }

    /** See class javadocs. */
    protected static void assertEquals(String msg, byte exp, byte actual) {
        Assert.assertEquals(msg, exp, actual);
    }

    /** See class javadocs. */
    protected static void assertEquals(byte exp, byte actual) {
        Assert.assertEquals(exp, actual);
    }

    /** See class javadocs. */
    protected static void assertEquals(char exp, char actual) {
        Assert.assertEquals(exp, actual);
    }

    /** See class javadocs. */
    protected static void assertEquals(short exp, short actual) {
        Assert.assertEquals(exp, actual);
    }

    /** See class javadocs. */
    protected static void assertNotSame(String msg, Object exp, Object actual) {
        Assert.assertNotSame(msg, exp, actual);
    }
}
