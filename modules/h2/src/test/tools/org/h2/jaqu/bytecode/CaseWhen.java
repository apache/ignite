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
 * A conditional expression.
 */
public class CaseWhen implements Token {

    private final Token condition, ifTrue, ifFalse;

    private CaseWhen(Token condition, Token ifTrue, Token ifFalse) {
        this.condition = condition;
        this.ifTrue = ifTrue;
        this.ifFalse = ifFalse;
    }

    static Token get(Token condition, Token ifTrue, Token ifFalse) {
        if ("0".equals(ifTrue.toString()) && "1".equals(ifFalse.toString())) {
            return Not.get(condition);
        } else if ("1".equals(ifTrue.toString()) && "0".equals(ifFalse.toString())) {
            return condition;
        } else if ("0".equals(ifTrue.toString())) {
            return And.get(Not.get(condition), ifFalse);
        }
        return new CaseWhen(condition, ifTrue, ifFalse);
    }

    @Override
    public String toString() {
        return "CASEWHEN(" + condition + ", " + ifTrue + ", " + ifFalse + ")";
    }

    @Override
    public <T> void appendSQL(SQLStatement stat, Query<T> query) {
        stat.appendSQL("CASEWHEN ");
        condition.appendSQL(stat, query);
        stat.appendSQL(" THEN ");
        ifTrue.appendSQL(stat, query);
        stat.appendSQL(" ELSE ");
        ifFalse.appendSQL(stat, query);
        stat.appendSQL(" END");
    }

}
