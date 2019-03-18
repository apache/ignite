/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu.bytecode;

import org.h2.jaqu.Query;
import org.h2.jaqu.SQLStatement;
import org.h2.util.StringUtils;

/**
 * A string constant.
 */
public class ConstantString implements Constant {

    private final String value;

    private ConstantString(String value) {
        this.value = value;
    }

    static ConstantString get(String v) {
        return new ConstantString(v);
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public int intValue() {
        return 0;
    }

    @Override
    public <T> void appendSQL(SQLStatement stat, Query<T> query) {
        stat.appendSQL(StringUtils.quoteStringSQL(value));
    }

    @Override
    public Constant.Type getType() {
        return Constant.Type.STRING;
    }

}
