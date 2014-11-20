/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

/**
 * Hadoop counter.
 */
public interface GridHadoopCounter<T> {
    /**
     * Gets name.
     *
     * @return Name of the counter.
     */
    public String name();

    /**
     * Gets counter group.
     *
     * @return Counter group's name.
     */
    public String group();

    /**
     * Gets current value of this counter.
     *
     * @return Current value.
     */
    public T value();

    /**
     * Set this counter by the given value.
     *
     * @param val Value to set.
     */
    public void value(T val);

    /**
     * Append the given value to this counter.
     *
     * @param val Value to append/increase this counter by.
     */
    public void append(T val);
}
