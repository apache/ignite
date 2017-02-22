// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.math;

/**
 * Utility mapper that can be used to map arbitrary values types to and from double.
 */
public interface DoubleMapper<V> {
    /**
     *
     * @param v
     * @return
     */
    public V fromDouble(double v);

    /**
     *
     * @param v
     * @return
     */
    public double toDouble(V  v);
}
