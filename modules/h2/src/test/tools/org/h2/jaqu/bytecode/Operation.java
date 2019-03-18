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
 * A mathematical or comparison operation.
 */
class Operation implements Token {

    /**
     * The operation type.
     */
    enum Type {
        EQUALS("=") {
            @Override
            Type reverse() {
                return NOT_EQUALS;
            }
        },
        NOT_EQUALS("<>") {
            @Override
            Type reverse() {
                return EQUALS;
            }
        },
        BIGGER(">") {
            @Override
            Type reverse() {
                return SMALLER_EQUALS;
            }
        },
        BIGGER_EQUALS(">=") {
            @Override
            Type reverse() {
                return SMALLER;
            }
        },
        SMALLER_EQUALS("<=") {
            @Override
            Type reverse() {
                return BIGGER;
            }
        },
        SMALLER("<") {
            @Override
            Type reverse() {
                return BIGGER_EQUALS;
            }
        },
        ADD("+"),
        SUBTRACT("-"),
        MULTIPLY("*"),
        DIVIDE("/"),
        MOD("%");

        private final String name;

        Type(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

        Type reverse() {
            return null;
        }

    }

    private final Token left, right;
    private final Type op;

    private Operation(Token left, Type op, Token right) {
        this.left = left;
        this.op = op;
        this.right = right;
    }

    static Token get(Token left, Type op, Token right) {
        if (op == Type.NOT_EQUALS && "0".equals(right.toString())) {
            return left;
        }
        return new Operation(left, op, right);
    }

    @Override
    public String toString() {
        return left + " " + op + " " + right;
    }

    public Token reverse() {
        return get(left, op.reverse(), right);
    }

    @Override
    public <T> void appendSQL(SQLStatement stat, Query<T> query) {
        left.appendSQL(stat, query);
        stat.appendSQL(op.toString());
        right.appendSQL(stat, query);
    }

}
