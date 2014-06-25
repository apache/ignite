/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.gridgain.grid.hadoop.*;

/**
 * Hadoop partitioner adapter for v2 API.
 */
public class GridHadoopV2Partitioner implements GridHadoopPartitioner {
    /** Partitioner instance. */
    private Partitioner<Object, Object> part;

    /**
     * @param cls Hadoop partitioner class.
     * @param conf Job configuration.
     */
    public GridHadoopV2Partitioner(Class<? extends Partitioner<?, ?>> cls, Configuration conf) {
        part = (Partitioner<Object, Object>) ReflectionUtils.newInstance(cls, conf);
    }

    /** {@inheritDoc} */
    @Override public int partition(Object key, Object val, int parts) {
        return part.getPartition(key, val, parts);
    }
}
