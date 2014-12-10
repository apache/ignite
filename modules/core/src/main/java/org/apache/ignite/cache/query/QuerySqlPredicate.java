// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.cache.query;

import org.gridgain.grid.util.typedef.internal.*;

import javax.cache.*;

/**
 * Query SQL predicate to use with any of the {@code JCache.query(...)} and
 * {@code JCache.queryFields(...)} methods.
 *
 * @author @java.author
 * @version @java.version
 */
public final class QuerySqlPredicate<K, V> extends QueryPredicate<K, V> {
    /** SQL clause. */
    private String sql;

    /** Arguments. */
    private Object[] args;

    /**
     * Empty constructor.
     */
    public QuerySqlPredicate() {
        // No-op.
    }

    /**
     * Constructs SQL predicate with given SQL clause and arguments.
     *
     * @param sql SQL clause.
     * @param args Arguments.
     */
    public QuerySqlPredicate(String sql, Object... args) {
        this.sql = sql;
        this.args = args;
    }

    /**
     * Constructs SQL predicate with given SQL clause, page size, and arguments.
     *
     * @param sql SQL clause.
     * @param pageSize Optional page size, if {@code 0}, then {@link QueryConfiguration#getPageSize()} is used.
     * @param args Arguments.
     */
    public QuerySqlPredicate(String sql, int pageSize, Object[] args) {
        super(pageSize);

        this.sql = sql;
        this.args = args;
    }

    /**
     * Gets SQL clause.
     *
     * @return SQL clause.
     */
    public String getSql() {
        return sql;
    }

    /**
     * Sets SQL clause.
     *
     * @param sql SQL clause.
     */
    public void setSql(String sql) {
        this.sql = sql;
    }

    /**
     * Gets SQL arguments.
     *
     * @return SQL arguments.
     */
    public Object[] getArgs() {
        return args;
    }

    /**
     * Sets SQL arguments.
     *
     * @param args SQL arguments.
     */
    public void setArgs(Object... args) {
        this.args = args;
    }

    /** {@inheritDoc} */
    @Override public boolean apply(Cache.Entry<K, V> entry) {
        return false; // Not used.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QuerySqlPredicate.class, this);
    }
}
