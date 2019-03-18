/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu.bytecode;

import org.h2.jaqu.Query;
import org.h2.jaqu.SQLStatement;
import org.h2.jaqu.Token;

/**
 * The Java 'null'.
 */
public class Null implements Token {

    static final Null INSTANCE = new Null();

    private Null() {
        // don't allow to create new instances
    }

    @Override
    public String toString() {
        return "null";
    }

    @Override
    public <T> void appendSQL(SQLStatement stat, Query<T> query) {
        // untested
        stat.appendSQL("NULL");
    }

}
