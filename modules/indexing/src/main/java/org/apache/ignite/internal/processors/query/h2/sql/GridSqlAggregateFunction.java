/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.query.h2.sql;

import org.h2.util.*;

import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.*;

/**
 * Aggregate function.
 */
public class GridSqlAggregateFunction extends GridSqlFunction {
    /** */
    private static final GridSqlFunctionType[] TYPE_INDEX = new GridSqlFunctionType[]{
        COUNT_ALL, COUNT, GROUP_CONCAT, SUM, MIN, MAX, AVG,
//        STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP, BOOL_OR, BOOL_AND, SELECTIVITY, HISTOGRAM,
    };

    /** */
    private final boolean distinct;

    /**
     * @param distinct Distinct.
     * @param type Type.
     */
    public GridSqlAggregateFunction(boolean distinct, GridSqlFunctionType type) {
        super(type);

        this.distinct = distinct;
    }

    /**
     * @param distinct Distinct.
     * @param typeId Type.
     */
    public GridSqlAggregateFunction(boolean distinct, int typeId) {
        this(distinct, TYPE_INDEX[typeId]);
    }

    /**
     * @return Distinct.
     */
    public boolean distinct() {
        return distinct;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        String text;

        switch (type) {
            case GROUP_CONCAT:
                throw new UnsupportedOperationException();

            case COUNT_ALL:
                return "COUNT(*)";

            default:
                text = type.name();

                break;
        }

        if (distinct)
            return text + "(DISTINCT " + child().getSQL() + ")";

        return text + StringUtils.enclose(child().getSQL());
    }
}
