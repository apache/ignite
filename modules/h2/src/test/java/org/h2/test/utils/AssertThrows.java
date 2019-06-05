/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

import java.lang.reflect.Method;
import java.sql.SQLException;
import org.h2.message.DbException;

/**
 * Helper class to simplify negative testing. Usage:
 * <pre>
 * new AssertThrows() { public void test() {
 *     Integer.parseInt("not a number");
 * }};
 * </pre>
 */
public abstract class AssertThrows {

    /**
     * Create a new assertion object, and call the test method to verify the
     * expected exception is thrown.
     *
     * @param expectedExceptionClass the expected exception class
     */
    public AssertThrows(final Class<? extends Exception> expectedExceptionClass) {
        this(new ResultVerifier() {
            @Override
            public boolean verify(Object returnValue, Throwable t, Method m,
                    Object... args) {
                if (t == null) {
                    throw new AssertionError("Expected an exception of type " +
                            expectedExceptionClass.getSimpleName() +
                            " to be thrown, but the method returned successfully");
                }
                if (!expectedExceptionClass.isAssignableFrom(t.getClass())) {
                    AssertionError ae = new AssertionError(
                            "Expected an exception of type\n" +
                                    expectedExceptionClass.getSimpleName() +
                                    " to be thrown, but the method under test " +
                                    "threw an exception of type\n" +
                                    t.getClass().getSimpleName() +
                                    " (see in the 'Caused by' for the exception " +
                                    "that was thrown)");
                    ae.initCause(t);
                    throw ae;
                }
                return false;
            }
        });
    }

    /**
     * Create a new assertion object, and call the test method to verify the
     * expected exception is thrown.
     */
    public AssertThrows() {
        this(new ResultVerifier() {
            @Override
            public boolean verify(Object returnValue, Throwable t, Method m,
                    Object... args) {
                if (t != null) {
                    throw new AssertionError("Expected an exception " +
                            "to be thrown, but the method returned successfully");
                }
                // all exceptions are fine
                return false;
            }
        });
    }

    /**
     * Create a new assertion object, and call the test method to verify the
     * expected exception is thrown.
     *
     * @param expectedErrorCode the error code of the exception
     */
    public AssertThrows(final int expectedErrorCode) {
        this(new ResultVerifier() {
            @Override
            public boolean verify(Object returnValue, Throwable t, Method m,
                    Object... args) {
                int errorCode;
                if (t instanceof DbException) {
                    errorCode = ((DbException) t).getErrorCode();
                } else if (t instanceof SQLException) {
                    errorCode = ((SQLException) t).getErrorCode();
                } else {
                    errorCode = 0;
                }
                if (errorCode != expectedErrorCode) {
                    AssertionError ae = new AssertionError(
                            "Expected an SQLException or DbException with error code " +
                            expectedErrorCode);
                    ae.initCause(t);
                    throw ae;
                }
                return false;
            }
        });
    }

    private AssertThrows(ResultVerifier verifier) {
        try {
            test();
            verifier.verify(null, null, null);
        } catch (Exception e) {
            verifier.verify(null, e, null);
        }
    }

    /**
     * The test method that is called.
     *
     * @throws Exception the exception
     */
    public abstract void test() throws Exception;

}
