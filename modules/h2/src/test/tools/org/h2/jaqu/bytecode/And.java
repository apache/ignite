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
 * An AND expression.
 */
public class And implements Token {

    private final Token left, right;

    private And(Token left, Token right) {
        this.left = left;
        this.right = right;
    }

    static And get(Token left, Token right) {
        return new And(left, right);
    }

    @Override
    public <T> void appendSQL(SQLStatement stat, Query<T> query) {
        left.appendSQL(stat, query);
        stat.appendSQL(" AND ");
        right.appendSQL(stat, query);
    }

}
