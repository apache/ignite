/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.sql;

import org.h2.value.*;

/**
 * Constant value.
 */
public class GridValueExpression extends GridSqlElement implements GridSqlValue {
    /** */
    private final Value val;

    /**
     * @param val Value.
     */
    public GridValueExpression(Value val) {
        this.val = val;
    }

    /**
     * @return Value.
     */
    public Value value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return val.getSQL();
    }
}
