/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle.collections;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Hash multimap.
 */
public class GridHadoopHashMultimap extends GridHadoopHashMultimapBase {
    /** */
    private long[] tbl;

    /**
     * @param job Job.
     * @param mem Memory.
     * @param cap Initial capacity.
     */
    public GridHadoopHashMultimap(GridHadoopJob job, GridUnsafeMemory mem, int cap) {
        super(job, mem);

        assert U.isPow2(cap) : cap;

        tbl = new long[cap];
    }

    /** {@inheritDoc} */
    @Override public Adder startAdding() throws GridException {
        return new AdderBase() {
            @Override public void write(Object key, Object val) throws GridException {
                // TODO
            }
        };
    }

    /** {@inheritDoc} */
    @Override public int capacity() {
        return tbl.length;
    }

    /** {@inheritDoc} */
    @Override protected long meta(int idx) {
        return tbl[idx];
    }
}
