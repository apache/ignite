/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.query.h2.sql;

import org.h2.expression.*;

/**
 * Full list of available functions see at {@link Function}
 */
public enum GridSqlFunctionType {
    // Aggregate functions.
    /** */
    COUNT_ALL("COUNT(*)"),

    /** */
    COUNT,

    /** */
    SUM,

    /** */
    MIN,

    /** */
    MAX,

    /** */
    AVG,

    /** */
    GROUP_CONCAT,

    // Functions with special handling.
    /** */
    CASE,

    /** */
    CAST,

    /** */
    CONVERT,

    /** */
    EXTRACT,

    /** Constant for all other functions. */
    UNKNOWN_FUNCTION;

    /** */
    private final String name;

    /**
     */
    GridSqlFunctionType() {
        name = name();
    }

    /**
     * @param name Name.
     */
    GridSqlFunctionType(String name) {
        this.name = name;
    }

    /**
     * @return Function name.
     */
    public String functionName() {
        return name;
    }
}
