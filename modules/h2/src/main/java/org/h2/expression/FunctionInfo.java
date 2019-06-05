/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression;

/**
 * This class contains information about a built-in function.
 */
class FunctionInfo {

    /**
     * The name of the function.
     */
    String name;

    /**
     * The function type.
     */
    int type;

    /**
     * The data type of the return value.
     */
    int returnDataType;

    /**
     * The number of parameters.
     */
    int parameterCount;

    /**
     * If the result of the function is NULL if any of the parameters is NULL.
     */
    boolean nullIfParameterIsNull;

    /**
     * If this function always returns the same value for the same parameters.
     */
    boolean deterministic;

    /**
     * Should the return value ResultSet be buffered in a local temporary file?
     */
    boolean bufferResultSetToLocalTemp = true;

}
