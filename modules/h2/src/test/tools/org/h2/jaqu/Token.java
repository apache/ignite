/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.jaqu;

/**
 * Classes implementing this interface can be used as a token in a statement.
 */
public interface Token {
    /**
     * Append the SQL to the given statement using the given query.
     *
     * @param stat the statement to append the SQL to
     * @param query the query to use
     */
    <T> void appendSQL(SQLStatement stat, Query<T> query);
}
