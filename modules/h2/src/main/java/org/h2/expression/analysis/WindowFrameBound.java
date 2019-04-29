/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.analysis;

import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.table.ColumnResolver;

/**
 * Window frame bound.
 */
public class WindowFrameBound {

    private final WindowFrameBoundType type;

    private Expression value;

    private boolean isVariable;

    private int expressionIndex = -1;

    /**
     * Creates new instance of window frame bound.
     *
     * @param type
     *            bound type
     * @param value
     *            bound value, if any
     */
    public WindowFrameBound(WindowFrameBoundType type, Expression value) {
        this.type = type;
        if (type == WindowFrameBoundType.PRECEDING || type == WindowFrameBoundType.FOLLOWING) {
            this.value = value;
        } else {
            this.value = null;
        }
    }

    /**
     * Returns the type
     *
     * @return the type
     */
    public WindowFrameBoundType getType() {
        return type;
    }

    /**
     * Returns the value.
     *
     * @return the value
     */
    public Expression getValue() {
        return value;
    }

    /**
     * Returns whether bound is defined as n PRECEDING or n FOLLOWING.
     *
     * @return whether bound is defined as n PRECEDING or n FOLLOWING
     */
    public boolean isParameterized() {
        return type == WindowFrameBoundType.PRECEDING || type == WindowFrameBoundType.FOLLOWING;
    }

    /**
     * Returns whether bound is defined with a variable. This method may be used
     * only after {@link #optimize(Session)} invocation.
     *
     * @return whether bound is defined with a variable
     */
    public boolean isVariable() {
        return isVariable;
    }

    /**
     * Returns the index of preserved expression.
     *
     * @return the index of preserved expression, or -1
     */
    public int getExpressionIndex() {
        return expressionIndex;
    }

    /**
     * Sets the index of preserved expression.
     *
     * @param expressionIndex
     *            the index to set
     */
    void setExpressionIndex(int expressionIndex) {
        this.expressionIndex = expressionIndex;
    }

    /**
     * Map the columns of the resolver to expression columns.
     *
     * @param resolver
     *            the column resolver
     * @param level
     *            the subquery nesting level
     * @param state
     *            current state for nesting checks
     */
    void mapColumns(ColumnResolver resolver, int level, int state) {
        if (value != null) {
            value.mapColumns(resolver, level, state);
        }
    }

    /**
     * Try to optimize bound expression.
     *
     * @param session
     *            the session
     */
    void optimize(Session session) {
        if (value != null) {
            value = value.optimize(session);
            if (!value.isConstant()) {
                isVariable = true;
            }
        }
    }

    /**
     * Update an aggregate value.
     *
     * @param session
     *            the session
     * @param stage
     *            select stage
     * @see Expression#updateAggregate(Session, int)
     */
    void updateAggregate(Session session, int stage) {
        if (value != null) {
            value.updateAggregate(session, stage);
        }
    }

    /**
     * Appends SQL representation to the specified builder.
     *
     * @param builder
     *            string builder
     * @param following
     *            if false return SQL for starting clause, if true return SQL
     *            for following clause
     * @param alwaysQuote
     *            quote all identifiers
     * @return the specified string builder
     * @see Expression#getSQL(StringBuilder, boolean)
     */
    public StringBuilder getSQL(StringBuilder builder, boolean following, boolean alwaysQuote) {
        if (type == WindowFrameBoundType.PRECEDING || type == WindowFrameBoundType.FOLLOWING) {
            value.getSQL(builder, alwaysQuote).append(' ');
        }
        return builder.append(type.getSQL());
    }

}
