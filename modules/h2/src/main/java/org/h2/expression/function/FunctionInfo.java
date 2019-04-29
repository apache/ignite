/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.function;

/**
 * This class contains information about a built-in function.
 */
public final class FunctionInfo {

    /**
     * The name of the function.
     */
    public final String name;

    /**
     * The function type.
     */
    public final int type;

    /**
     * The number of parameters.
     */
    final int parameterCount;

    /**
     * The data type of the return value.
     */
    public final int returnDataType;

    /**
     * If the result of the function is NULL if any of the parameters is NULL.
     */
    final boolean nullIfParameterIsNull;

    /**
     * If this function always returns the same value for the same parameters.
     */
    public final boolean deterministic;

    /**
     * Should the return value ResultSet be buffered in a local temporary file?
     */
    final boolean bufferResultSetToLocalTemp;

    /**
     * Should the no-arg function require parentheses.
     */
    final boolean requireParentheses;

    /**
     * Creates new instance of built-in function information.
     *
     * @param name
     *            the name of the function
     * @param type
     *            the function type
     * @param parameterCount
     *            the number of parameters
     * @param returnDataType
     *            the data type of the return value
     * @param nullIfParameterIsNull
     *            if the result of the function is NULL if any of the parameters
     *            is NULL
     * @param deterministic
     *            if this function always returns the same value for the same
     *            parameters
     * @param bufferResultSetToLocalTemp
     *            should the return value ResultSet be buffered in a local
     *            temporary file?
     * @param requireParentheses
     *            should the no-arg function require parentheses
     */
    public FunctionInfo(String name, int type, int parameterCount, int returnDataType, boolean nullIfParameterIsNull,
            boolean deterministic, boolean bufferResultSetToLocalTemp, boolean requireParentheses) {
        this.name = name;
        this.type = type;
        this.parameterCount = parameterCount;
        this.returnDataType = returnDataType;
        this.nullIfParameterIsNull = nullIfParameterIsNull;
        this.deterministic = deterministic;
        this.bufferResultSetToLocalTemp = bufferResultSetToLocalTemp;
        this.requireParentheses = requireParentheses;
    }

    /**
     * Creates a copy of built-in function information with a different name. A
     * copy will require parentheses.
     *
     * @param source
     *            the source information
     * @param name
     *            the new name
     */
    public FunctionInfo(FunctionInfo source, String name) {
        this.name = name;
        type = source.type;
        returnDataType = source.returnDataType;
        parameterCount = source.parameterCount;
        nullIfParameterIsNull = source.nullIfParameterIsNull;
        deterministic = source.deterministic;
        bufferResultSetToLocalTemp = source.bufferResultSetToLocalTemp;
        requireParentheses = true;
    }

}
