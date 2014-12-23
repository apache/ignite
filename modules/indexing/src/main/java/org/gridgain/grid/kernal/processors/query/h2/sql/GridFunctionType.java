/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.sql;

import org.h2.expression.*;

/**
 * Full list of available functions see at {@link Function}
 */
public enum GridFunctionType {
    ABS(1), CEIL(1), FLOOR(1), COS(1), PI(0), POWER(2), RAND(-1), ROUND(1),
    CASE(-1), CAST(1), CONVERT(1), EXTRACT(2),
    DAY_OF_MONTH(1), DAY_OF_WEEK(1), DAY_OF_YEAR(1),

    // Aggregate functions.
    COUNT_ALL("COUNT(*)", 0), COUNT(1), GROUP_CONCAT(1), SUM(1), MIN(1), MAX(1), AVG(1), STDDEV_POP(1), STDDEV_SAMP(1),
    VAR_POP(1), VAR_SAMP(1), BOOL_OR(1), BOOL_AND(1), SELECTIVITY(1), HISTOGRAM(1);

    /** */
    private final String name;

    /** */
    private final int argCnt;

    /**
     * @param argCnt Count.
     */
    GridFunctionType(int argCnt) {
        name = name();
        this.argCnt = argCnt;
    }

    /**
     * @param name Name.
     * @param argCnt Count.
     */
    GridFunctionType(String name, int argCnt) {
        this.name = name;
        this.argCnt = argCnt;
    }

    /**
     * @return Argument count.
     */
    public int argumentCount() {
        return argCnt;
    }

    /**
     * @return Function name.
     */
    public String functionName() {
        return name;
    }
}
