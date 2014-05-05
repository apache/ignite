/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.mapreduce.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Hadoop partitioner adapter for v2 API.
 */
public class GridHadoopV2Partitioner implements GridHadoopPartitioner {
    /** Partitioner instance. */
    private Partitioner<Object, Object> part;

    /**
     * @param cls Hadoop partitioner class.
     */
    public GridHadoopV2Partitioner(Class<? extends Partitioner<?, ?>> cls) throws GridException {
        part = (Partitioner<Object, Object>) U.newInstance(cls);
    }

    /** {@inheritDoc} */
    @Override public int partition(Object key, Object val, int parts) {
        return part.getPartition(key, val, parts);
    }
}
