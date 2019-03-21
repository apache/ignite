/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.expression.analysis;

import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectOrderBy;
import org.h2.engine.Session;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.result.SortOrder;
import org.h2.table.ColumnResolver;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueRow;

/**
 * Window clause.
 */
public final class Window {

    private ArrayList<Expression> partitionBy;

    private ArrayList<SelectOrderBy> orderBy;

    private WindowFrame frame;

    private String parent;

    /**
     * Appends ORDER BY clause to the specified builder.
     *
     * @param builder
     *            string builder
     * @param orderBy
     *            ORDER BY clause, or null
     * @param alwaysQuote
     *            quote all identifiers
     */
    public static void appendOrderBy(StringBuilder builder, ArrayList<SelectOrderBy> orderBy, boolean alwaysQuote) {
        if (orderBy != null && !orderBy.isEmpty()) {
            if (builder.charAt(builder.length() - 1) != '(') {
                builder.append(' ');
            }
            builder.append("ORDER BY ");
            for (int i = 0; i < orderBy.size(); i++) {
                SelectOrderBy o = orderBy.get(i);
                if (i > 0) {
                    builder.append(", ");
                }
                o.expression.getSQL(builder, alwaysQuote);
                SortOrder.typeToString(builder, o.sortType);
            }
        }
    }

    /**
     * Creates a new instance of window clause.
     *
     * @param parent
     *            name of the parent window
     * @param partitionBy
     *            PARTITION BY clause, or null
     * @param orderBy
     *            ORDER BY clause, or null
     * @param frame
     *            window frame clause, or null
     */
    public Window(String parent, ArrayList<Expression> partitionBy, ArrayList<SelectOrderBy> orderBy,
            WindowFrame frame) {
        this.parent = parent;
        this.partitionBy = partitionBy;
        this.orderBy = orderBy;
        this.frame = frame;
    }

    /**
     * Map the columns of the resolver to expression columns.
     *
     * @param resolver
     *            the column resolver
     * @param level
     *            the subquery nesting level
     * @see Expression#mapColumns(ColumnResolver, int, int)
     */
    public void mapColumns(ColumnResolver resolver, int level) {
        resolveWindows(resolver);
        if (partitionBy != null) {
            for (Expression e : partitionBy) {
                e.mapColumns(resolver, level, Expression.MAP_IN_WINDOW);
            }
        }
        if (orderBy != null) {
            for (SelectOrderBy o : orderBy) {
                o.expression.mapColumns(resolver, level, Expression.MAP_IN_WINDOW);
            }
        }
        if (frame != null) {
            frame.mapColumns(resolver, level, Expression.MAP_IN_WINDOW);
        }
    }

    private void resolveWindows(ColumnResolver resolver) {
        if (parent != null) {
            Select select = resolver.getSelect();
            Window p;
            while ((p = select.getWindow(parent)) == null) {
                select = select.getParentSelect();
                if (select == null) {
                    throw DbException.get(ErrorCode.WINDOW_NOT_FOUND_1, parent);
                }
            }
            p.resolveWindows(resolver);
            if (partitionBy == null) {
                partitionBy = p.partitionBy;
            }
            if (orderBy == null) {
                orderBy = p.orderBy;
            }
            if (frame == null) {
                frame = p.frame;
            }
            parent = null;
        }
    }

    /**
     * Try to optimize the window conditions.
     *
     * @param session
     *            the session
     */
    public void optimize(Session session) {
        if (partitionBy != null) {
            for (int i = 0; i < partitionBy.size(); i++) {
                partitionBy.set(i, partitionBy.get(i).optimize(session));
            }
        }
        if (orderBy != null) {
            for (SelectOrderBy o : orderBy) {
                o.expression = o.expression.optimize(session);
            }
        }
        if (frame != null) {
            frame.optimize(session);
        }
    }

    /**
     * Tell the expression columns whether the table filter can return values
     * now. This is used when optimizing the query.
     *
     * @param tableFilter
     *            the table filter
     * @param value
     *            true if the table filter can return value
     * @see Expression#setEvaluatable(TableFilter, boolean)
     */
    public void setEvaluatable(TableFilter tableFilter, boolean value) {
        if (partitionBy != null) {
            for (Expression e : partitionBy) {
                e.setEvaluatable(tableFilter, value);
            }
        }
        if (orderBy != null) {
            for (SelectOrderBy o : orderBy) {
                o.expression.setEvaluatable(tableFilter, value);
            }
        }
    }

    /**
     * Returns ORDER BY clause.
     *
     * @return ORDER BY clause, or null
     */
    public ArrayList<SelectOrderBy> getOrderBy() {
        return orderBy;
    }

    /**
     * Returns window frame, or null.
     *
     * @return window frame, or null
     */
    public WindowFrame getWindowFrame() {
        return frame;
    }

    /**
     * Returns the key for the current group.
     *
     * @param session
     *            session
     * @return key for the current group, or null
     */
    public Value getCurrentKey(Session session) {
        if (partitionBy == null) {
            return null;
        }
        int len = partitionBy.size();
        if (len == 1) {
            return partitionBy.get(0).getValue(session);
        } else {
            Value[] keyValues = new Value[len];
            // update group
            for (int i = 0; i < len; i++) {
                Expression expr = partitionBy.get(i);
                keyValues[i] = expr.getValue(session);
            }
            return ValueRow.get(keyValues);
        }
    }

    /**
     * Appends SQL representation to the specified builder.
     *
     * @param builder
     *            string builder
     * @param alwaysQuote quote all identifiers
     * @return the specified string builder
     * @see Expression#getSQL(StringBuilder, boolean)
     */
    public StringBuilder getSQL(StringBuilder builder, boolean alwaysQuote) {
        builder.append("OVER (");
        if (partitionBy != null) {
            builder.append("PARTITION BY ");
            for (int i = 0; i < partitionBy.size(); i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                partitionBy.get(i).getUnenclosedSQL(builder, alwaysQuote);
            }
        }
        appendOrderBy(builder, orderBy, alwaysQuote);
        if (frame != null) {
            if (builder.charAt(builder.length() - 1) != '(') {
                builder.append(' ');
            }
            frame.getSQL(builder, alwaysQuote);
        }
        return builder.append(')');
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
    public void updateAggregate(Session session, int stage) {
        if (partitionBy != null) {
            for (Expression expr : partitionBy) {
                expr.updateAggregate(session, stage);
            }
        }
        if (orderBy != null) {
            for (SelectOrderBy o : orderBy) {
                o.expression.updateAggregate(session, stage);
            }
        }
        if (frame != null) {
            frame.updateAggregate(session, stage);
        }
    }

    @Override
    public String toString() {
        return getSQL(new StringBuilder(), false).toString();
    }

}
