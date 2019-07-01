/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.db;

import java.io.Serializable;

/**
 * A utility class for TestLob.
 */
class TestLobObject implements Serializable {

    private static final long serialVersionUID = 1L;
    String data;

    TestLobObject(String data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "TestLobObject: " + data;
    }
}
