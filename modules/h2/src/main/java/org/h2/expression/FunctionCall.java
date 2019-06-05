/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

import org.h2.engine.Session;
import org.h2.value.ValueResultSet;

/**
 * This interface is used by the built-in functions,
 * as well as the user-defined functions.
 */
public interface FunctionCall {

    /**
     * Get the name of the function.
     *
     * @return the name
     */
    String getName();

    /**
     * Get an empty result set with the column names set.
     *
     * @param session the session
     * @param nullArgs the argument list (some arguments may be null)
     * @return the empty result set
     */
    ValueResultSet getValueForColumnList(Session session, Expression[] nullArgs);

    /**
     * Get the data type.
     *
     * @return the data type
     */
    int getType();

    /**
     * Optimize the function if possible.
     *
     * @param session the session
     * @return the optimized expression
     */
    Expression optimize(Session session);

    /**
     * Get the function arguments.
     *
     * @return argument list
     */
    Expression[] getArgs();

    /**
     * Get the SQL snippet of the function (including arguments).
     *
     * @return the SQL snippet.
     */
    String getSQL();

    /**
     * Whether the function always returns the same result for the same
     * parameters.
     *
     * @return true if it does
     */
    boolean isDeterministic();

    /**
     * Should the return value ResultSet be buffered in a local temporary file?
     *
     * @return true if it should be.
     */
    boolean isBufferResultSetToLocalTemp();

}
