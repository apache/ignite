/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

import java.lang.reflect.Method;

/**
 * This handler is called after a method returned.
 */
public interface ResultVerifier {

    /**
     * Verify the result or exception.
     *
     * @param returnValue the returned value or null
     * @param t the exception / error or null if the method returned normally
     * @param m the method or null if unknown
     * @param args the arguments or null if unknown
     * @return true if the method should be called again
     */
    boolean verify(Object returnValue, Throwable t, Method m, Object... args);

}
