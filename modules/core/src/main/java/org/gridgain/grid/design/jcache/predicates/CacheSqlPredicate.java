// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.design.jcache.predicates;

import org.gridgain.grid.util.typedef.internal.*;

import javax.cache.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class CacheSqlPredicate<K, V> implements CachePredicate<K, V> {
    /** SQL clause. */
    private String sql;

    /** Arguments. */
    private Object[] args;

    public CacheSqlPredicate(String sql, Object... args) {
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
    @Override public final boolean apply(Cache.Entry<K, V> entry) {
        return false; // Not used.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheSqlPredicate.class, this);
    }
}
