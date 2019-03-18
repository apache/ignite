/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test;

import org.junit.Test;

/**
 * This class is a bridge between JUnit and the custom test framework
 * used by H2.
 */
public class TestAllJunit {

    /**
     * Run all the fast tests.
     */
    @Test
    public void testTravis() throws Exception {
        TestAll.main("travis");
    }
}
