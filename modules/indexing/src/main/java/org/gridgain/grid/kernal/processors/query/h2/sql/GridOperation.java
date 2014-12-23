/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.sql;

import org.jetbrains.annotations.*;

/**
 *
 */
public class GridOperation extends GridSqlElement {
    /** */
    private final GridOperationType opType;

    /**
     * @param opType Operation type.
     */
    public GridOperation(@NotNull GridOperationType opType) {
        this.opType = opType;
    }

    /**
     * @param opType Op type.
     * @param arg argument.
     */
    public GridOperation(GridOperationType opType, GridSqlElement arg) {
        this(opType);

        addChild(arg);
    }

    /**
     * @param opType Op type.
     * @param left Left.
     * @param right Right.
     */
    public GridOperation(GridOperationType opType, GridSqlElement left, GridSqlElement right) {
        this(opType);

        addChild(left);
        addChild(right);
    }

    /**
     * @return Left.
     */
    public GridSqlElement left() {
        return child(0);
    }

    /**
     * @return Right.
     */
    public GridSqlElement right() {
        return child(1);
    }

    /**
     * @return Operation type.
     */
    public GridOperationType opType() {
        return opType;
    }

    /** {@inheritDoc} */
    @Override public String getSQL() {
        return opType.toSql(this);
    }
}
