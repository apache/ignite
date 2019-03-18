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
 * A variable.
 */
public class Variable implements Token {

    static final Variable THIS = new Variable("this", null);

    private final String name;
    private final Object obj;

    private Variable(String name, Object obj) {
        this.name = name;
        this.obj = obj;
    }

    static Variable get(String name, Object obj) {
        return new Variable(name, obj);
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public <T> void appendSQL(SQLStatement stat, Query<T> query) {
        query.appendSQL(stat, obj);
    }

}
