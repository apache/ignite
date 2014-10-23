// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.cache.query;

import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.internal.*;

import javax.cache.*;

/**
 * Query predicate to pass into any of {@code Cache.query(...)} methods.
 * Use {@link QuerySqlPredicate} and {@link QueryTextPredicate} for SQL and
 * text queries accordingly.
 *
 * @author @java.author
 * @version @java.version
 */
public abstract class QueryPredicate<K, V> implements GridPredicate<Cache.Entry<K, V>> {
    /** Page size. */
    private int pageSize;

    /**
     * Empty constructor.
     */
    protected QueryPredicate() {
        // No-op.
    }

    /**
     * Constructs query predicate with optional page size, if {@code 0},
     * then {@link QueryConfiguration#getPageSize()} is used.
     *
     * @param pageSize Optional page size.
     */
    protected QueryPredicate(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * Gets optional page size, if {@code 0}, then {@link QueryConfiguration#getPageSize()} is used.
     *
     * @return Optional page size.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Sets optional page size, if {@code 0}, then {@link QueryConfiguration#getPageSize()} is used.
     *
     * @param pageSize Optional page size.
     */
    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryPredicate.class, this);
    }
}
