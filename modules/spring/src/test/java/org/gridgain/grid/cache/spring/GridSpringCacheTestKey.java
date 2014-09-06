/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.cache.spring;

/**
 * Complex key.
 */
public class GridSpringCacheTestKey {
    /** */
    private final Integer p1;

    /** */
    private final String p2;

    /**
     * @param p1 Parameter 1.
     * @param p2 Parameter 2.
     */
    public GridSpringCacheTestKey(Integer p1, String p2) {
        assert p1 != null;
        assert p2 != null;

        this.p1 = p1;
        this.p2 = p2;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridSpringCacheTestKey key = (GridSpringCacheTestKey)o;

        return p1.equals(key.p1) && p2.equals(key.p2);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * p1 + p2.hashCode();
    }
}
