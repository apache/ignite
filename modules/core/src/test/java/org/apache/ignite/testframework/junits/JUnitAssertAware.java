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
 * Provides the basic functionality of {@link Assert} methods in org.junit package.
 * Corresponding methods must be used in all ignite tests where necessary.
 */
class JUnitAssertAware {
    /** See {@link Assert#assertTrue(String, boolean)} javadocs. */
    protected static void assertTrue(String msg, boolean cond) {
        Assert.assertTrue(msg, cond);
    }

    /** See {@link Assert#assertTrue(boolean)} javadocs. */
    protected static void assertTrue(boolean cond) {
        Assert.assertTrue(cond);
    }

    /** See {@link Assert#assertFalse(String, boolean)} javadocs. */
    protected static void assertFalse(String msg, boolean cond) {
        Assert.assertFalse(msg, cond);
    }

    /** See {@link Assert#assertFalse(boolean)} javadocs. */
    protected static void assertFalse(boolean cond) {
        Assert.assertFalse(cond);
    }

    /** See {@link Assert#fail(String)} javadocs. */
    protected static void fail(String msg) {
        Assert.fail(msg);
    }

    /** See {@link Assert#fail()} javadocs. */
    protected static void fail() {
        Assert.fail();
    }

    /** See {@link Assert#assertEquals(Object, Object)} javadocs. */
    protected static void assertEquals(Object exp, Object actual) {
        Assert.assertEquals(exp, actual);
    }

    /** See {@link Assert#assertEquals(Object, Object)} javadocs. */
    protected static void assertEquals(String exp, String actual) {
        Assert.assertEquals(exp, actual);
    }

    /** See {@link Assert#assertEquals(Object, Object)} javadocs. */
    protected static void assertEquals(boolean exp, boolean actual) {
        Assert.assertEquals(exp, actual);
    }

    /** See {@link Assert#assertEquals(String, Object, Object)} javadocs. */
    protected static void assertEquals(String msg, Object exp, Object actual) {
        Assert.assertEquals(msg, exp, actual);
    }

    /** See {@link Assert#assertEquals(String, Object, Object)} javadocs. */
    protected static void assertEquals(String msg, String exp, String actual) {
        Assert.assertEquals(msg, exp, actual);
    }

    /** See {@link Assert#assertEquals(String, Object, Object)} javadocs. */
    protected static void assertEquals(String msg, boolean exp, boolean actual) {
        Assert.assertEquals(msg, exp, actual);
    }

    /** See {@link Assert#assertEquals(long, long)} javadocs. */
    protected static void assertEquals(long exp, long actual) {
        Assert.assertEquals(exp, actual);
    }

    /** See {@link Assert#assertEquals(long, long)} javadocs. */
    protected static void assertEquals(int exp, int actual) {
        Assert.assertEquals(exp, actual);
    }

    /** See {@link Assert#assertEquals(long, long)} javadocs. */
    protected static void assertEquals(byte exp, byte actual) {
        Assert.assertEquals(exp, actual);
    }

    /** See {@link Assert#assertEquals(long, long)} javadocs. */
    protected static void assertEquals(char exp, char actual) {
        Assert.assertEquals(exp, actual);
    }

    /** See {@link Assert#assertEquals(long, long)} javadocs. */
    protected static void assertEquals(short exp, short actual) {
        Assert.assertEquals(exp, actual);
    }

    /** See {@link Assert#assertEquals(String, long, long)} javadocs. */
    protected static void assertEquals(String msg, long exp, long actual) {
        Assert.assertEquals(msg, exp, actual);
    }

    /** See {@link Assert#assertEquals(String, long, long)} javadocs. */
    protected static void assertEquals(String msg, int exp, int actual) {
        Assert.assertEquals(msg, exp, actual);
    }

    /** See {@link Assert#assertEquals(double, double, double)} javadocs. */
    protected static void assertEquals(double exp, double actual, double delta) {
        Assert.assertEquals(exp, actual, delta);
    }

    /** See {@link Assert#assertEquals(double, double, double)} javadocs. */
    protected static void assertEquals(double exp, Double actual) {
        Assert.assertEquals(exp, actual, 0);
    }

    /** See {@link Assert#assertEquals(double, double, double)} javadocs. */
    protected static void assertEquals(Double exp, double actual) {
        Assert.assertEquals(exp, actual, 0);
    }

    /** See {@link Assert#assertEquals(double, double, double)} javadocs. */
    protected static void assertEquals(double exp, double actual) {
        Assert.assertEquals(exp, actual, 0);
    }

    /** See {@link Assert#assertEquals(String, double, double, double)} javadocs. */
    protected static void assertEquals(String msg, double exp, double actual, double delta) {
        Assert.assertEquals(msg, exp, actual, delta);
    }

    /** See {@link Assert#assertEquals(String, double, double, double)} javadocs. */
    protected static void assertEquals(String msg, double exp, double actual) {
        Assert.assertEquals(msg, exp, actual, 0);
    }

    /** See {@link Assert#assertEquals(float, float, float)} javadocs. */
    protected static void assertEquals(float exp, float actual, float delta) {
        Assert.assertEquals(exp, actual, delta);
    }

    /** See {@link Assert#assertEquals(float, float, float)} javadocs. */
    protected static void assertEquals(float exp, float actual) {
        Assert.assertEquals(exp, actual, 0);
    }

    /** See {@link Assert#assertEquals(float, float, float)} javadocs. */
    protected static void assertEquals(float exp, Float actual) {
        Assert.assertEquals(exp, actual, 0);
    }

    /** See {@link Assert#assertEquals(String, long, long)} javadocs. */
    protected static void assertEquals(String msg, byte exp, byte actual) {
        Assert.assertEquals(msg, exp, actual);
    }

    /** See {@link Assert#assertNull(Object)} javadocs. */
    protected static void assertNull(Object obj) {
        Assert.assertNull(obj);
    }

    /** See {@link Assert#assertNull(Object)} javadocs. */
    protected static void assertNotNull(Object obj) {
        Assert.assertNotNull(obj);
    }

    /** See {@link Assert#assertNotNull(String, Object)} javadocs. */
    protected static void assertNotNull(String msg, Object obj) {
        Assert.assertNotNull(msg, obj);
    }

    /** See {@link Assert#assertNull(String, Object)} javadocs. */
    protected static void assertNull(String msg, Object obj) {
        Assert.assertNull(msg, obj);
    }

    /** See {@link Assert#assertSame(Object, Object)} javadocs. */
    protected static void assertSame(Object exp, Object actual) {
        Assert.assertSame(exp, actual);
    }

    /** See {@link Assert#assertNotSame(Object, Object)} javadocs. */
    protected static void assertNotSame(Object unexpected, Object actual) {
        Assert.assertNotSame(unexpected, actual);
    }

    /** See {@link Assert#assertNotSame(String, Object, Object)} javadocs. */
    protected static void assertNotSame(String msg, Object exp, Object actual) {
        Assert.assertNotSame(msg, exp, actual);
    }
}
