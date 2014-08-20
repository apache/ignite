/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

/**
 * Partitioner.
 */
public interface GridHadoopPartitioner {
    /**
     * Gets partition which is actually a reducer index for the given key and value pair.
     *
     * @param key Key.
     * @param val Value.
     * @param parts Number of partitions.
     * @return Partition.
     */
    public int partition(Object key, Object val, int parts);
}
