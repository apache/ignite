/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.apache.hadoop.io.*;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.hadoop.*;

import java.io.*;

import org.jetbrains.annotations.*;

/**
 * The wrapper for native hadoop input splits.
 */
public class GridHadoopSplitWrapper implements GridHadoopInputSplit {
    /** Hosts where split is located. */
    private String[] hosts;

    /** Native hadoop input split. */
    private Writable innerSplit;

    /**
     * Creates new split wrapper.
     */
    public GridHadoopSplitWrapper() {
        // No-op.
    }

    /**
     * Creates new split wrapper.
     *
     * @param innerSplit Native hadoop input split to wrap.
     * @param hosts Hosts where split is located.
     */
    public GridHadoopSplitWrapper(Writable innerSplit, String[] hosts) {
        A.notNull(innerSplit, "innerSplit", hosts, "hosts");

        this.innerSplit = innerSplit;
        this.hosts = hosts;
    }

    /** {@inheritDoc} */
    @Override public String[] hosts() {
        assert hosts != null;

        return hosts;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object innerSplit() {
        return innerSplit;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(innerSplit.getClass());

        innerSplit.write(out);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        Class<Writable> cls = (Class<Writable>)in.readObject();

        try {
            innerSplit = U.newInstance(cls);

            innerSplit.readFields(in);
        }
        catch (GridException e) {
            throw new IOException(e);
        }
    }
}
