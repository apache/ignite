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
 * A method call.
 */
class Function implements Token {

    private final String name;
    private final Token expr;

    Function(String name, Token expr) {
        this.name = name;
        this.expr = expr;
    }

    @Override
    public String toString() {
        return name + "(" + expr + ")";
    }

    @Override
    public <T> void appendSQL(SQLStatement stat, Query<T> query) {
        // untested
        stat.appendSQL(name + "(");
        expr.appendSQL(stat, query);
        stat.appendSQL(")");
    }
}
