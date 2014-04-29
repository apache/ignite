/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.hadoop1impl;

import org.apache.hadoop.mapred.*;
import org.gridgain.grid.hadoop.*;

/**
 * Hadoop partitioner adapter for v2 API.
 */
public class GridHadoopV1PartitionerAdapter implements GridHadoopPartitioner {
    /** Partitioner instance. */
    private Partitioner part;

    /**
     * @param part Hadoop partitioner.
     */
    public GridHadoopV1PartitionerAdapter(Partitioner part) {
        this.part = part;
    }

    /** {@inheritDoc} */
    @Override public int partition(Object key, Object val, int parts) {
        return part.getPartition(key, val, parts);
    }
}

