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
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public final class QueryTextPredicate<K, V> extends QueryPredicate<K, V> {
    /** SQL clause. */
    private String txt;

    /** Arguments. */
    private Object[] args;

    public QueryTextPredicate(String txt, Object... args) {
        this.txt = txt;
        this.args = args;
    }

    /**
     * Gets text search string.
     *
     * @return Text search string.
     */
    public String getText() {
        return txt;
    }

    /**
     * Sets text search string.
     *
     * @param txt Text search string.
     */
    public void setText(String txt) {
        this.txt = txt;
    }

    /**
     * Gets text search arguments.
     *
     * @return Text search arguments.
     */
    public Object[] getArgs() {
        return args;
    }

    /**
     * Sets text search arguments.
     *
     * @param args Text search arguments.
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
        return S.toString(QueryTextPredicate.class, this);
    }
}
