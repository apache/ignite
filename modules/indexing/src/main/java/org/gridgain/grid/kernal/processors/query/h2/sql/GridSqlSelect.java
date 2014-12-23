/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.query.h2.sql;

import org.h2.result.*;
import org.h2.util.*;

import java.util.*;

/**
 * Select query.
 */
public class GridSqlSelect implements Cloneable {
    /** */
    private boolean distinct;

    /** */
    private List<GridSqlElement> select = new ArrayList<>();

    /** */
    private List<GridSqlElement> groups = new ArrayList<>();

    /** */
    private GridSqlElement from;

    /** */
    private GridSqlElement where;

    /** */
    private GridSqlElement having;

    /** */
    private Map<GridSqlElement, Integer> sort = new LinkedHashMap<>();

    /**
     * @return Distinct.
     */
    public boolean distinct() {
        return distinct;
    }

    /**
     * @param distinct New distinct.
     */
    public void distinct(boolean distinct) {
        this.distinct = distinct;
    }

    /**
     * @return Generate sql.
     */
    public String getSQL() {
        StatementBuilder buff = new StatementBuilder("SELECT");

        if (distinct)
            buff.append(" DISTINCT");

        for (GridSqlElement expression : select) {
            buff.appendExceptFirst(",");
            buff.append('\n');
            buff.append(StringUtils.indent(expression.getSQL(), 4, false));
        }

        buff.append("\nFROM ").append(from.getSQL());

        if (where != null)
            buff.append("\nWHERE ").append(StringUtils.unEnclose(where.getSQL()));

        if (!groups.isEmpty()) {
            buff.append("\nGROUP BY ");

            buff.resetCount();

            for (GridSqlElement expression : groups) {
                buff.appendExceptFirst(", ");

                if (expression instanceof GridSqlAlias)
                    buff.append(StringUtils.unEnclose((expression.child().getSQL())));
                else
                    buff.append(StringUtils.unEnclose(expression.getSQL()));
            }
        }

        if (having != null)
            buff.append("\nHAVING ").append(StringUtils.unEnclose(having.getSQL()));

        if (!sort.isEmpty()) {
            buff.append("\nORDER BY ");

            buff.resetCount();

            for (Map.Entry<GridSqlElement, Integer> entry : sort.entrySet()) {
                buff.appendExceptFirst(", ");

                GridSqlElement expression = entry.getKey();

                int idx = select.indexOf(expression);

                if (idx >= 0)
                    buff.append(idx + 1);
                else
                    buff.append('=').append(StringUtils.unEnclose(expression.getSQL()));

                int type = entry.getValue();

                if ((type & SortOrder.DESCENDING) != 0)
                    buff.append(" DESC");

                if ((type & SortOrder.NULLS_FIRST) != 0)
                    buff.append(" NULLS FIRST");
                else if ((type & SortOrder.NULLS_LAST) != 0)
                    buff.append(" NULLS LAST");
            }
        }

        return buff.toString();
    }

    /**
     * @return Expressions.
     */
    public List<GridSqlElement> select() {
        return select;
    }

    /**
     * @param expression Expression.
     */
    public void addSelectExpression(GridSqlElement expression) {
        select.add(expression);
    }

    /**
     * @return Expressions.
     */
    public List<GridSqlElement> groups() {
        return groups;
    }

    /**
     *
     */
    public void clearGroups() {
        groups.clear();
    }

    /**
     * @param expression Expression.
     */
    public void addGroupExpression(GridSqlElement expression) {
        groups.add(expression);
    }

    /**
     * @return Tables.
     */
    public GridSqlElement from() {
        return from;
    }

    /**
     * @param from From element.
     */
    public void from(GridSqlElement from) {
        this.from = from;
    }

    /**
     * @return Where.
     */
    public GridSqlElement where() {
        return where;
    }

    /**
     * @param where New where.
     */
    public void where(GridSqlElement where) {
        this.where = where;
    }

    /**
     * @return Having.
     */
    public GridSqlElement having() {
        return having;
    }

    /**
     * @param having New having.
     */
    public void having(GridSqlElement having) {
        this.having = having;
    }

    /**
     * @return Sort.
     */
    public Map<GridSqlElement, Integer> sort() {
        return sort;
    }

    /**
     *
     */
    public void clearSort() {
        sort.clear();
    }

    /**
     * @param expression Expression.
     * @param sortType The sort type bit mask (SortOrder.DESCENDING, SortOrder.NULLS_FIRST, SortOrder.NULLS_LAST).
     */
    public void addSort(GridSqlElement expression, int sortType) {
        sort.put(expression, sortType);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneCallsConstructors", "CloneDoesntDeclareCloneNotSupportedException"})
    @Override public GridSqlSelect clone() {
        try {
            GridSqlSelect res = (GridSqlSelect)super.clone();

            res.select = new ArrayList<>(select);
            res.groups = new ArrayList<>(groups);
            res.sort = new LinkedHashMap<>(sort);

            return res;
        }
        catch (CloneNotSupportedException e) {
            throw new RuntimeException(e); // Never thrown.
        }
    }
}
