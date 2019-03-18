/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

/**
 * An enumeration of compare operations.
 */
enum CompareType {
    EQUAL("=", true),
    BIGGER(">", true),
    BIGGER_EQUAL(">=", true),
    SMALLER("<", true),
    SMALLER_EQUAL("<=", true),
    NOT_EQUAL("<>", true),
    IS_NOT_NULL("IS NOT NULL", false),
    IS_NULL("IS NULL", false),
    LIKE("LIKE", true);

    private String text;
    private boolean hasRightExpression;

    CompareType(String text, boolean hasRightExpression) {
        this.text = text;
        this.hasRightExpression = hasRightExpression;
    }

    String getString() {
        return text;
    }

    boolean hasRightExpression() {
        return hasRightExpression;
    }

}

