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
    private Object innerSplit;

    /**
     * Creates new split wrapper.
     */
    public GridHadoopSplitWrapper() {
        // No-op.
    }

    /**
     * Creates new split wrapper.
     *
     * @param innerSplit Native hadoop input split to wrap or {@code null} if it is serialized in external file.
     * @param hosts Hosts where split is located.
     */
    public GridHadoopSplitWrapper(@Nullable Object innerSplit, String[] hosts) {
        assert hosts != null;

        this.innerSplit = innerSplit;
        this.hosts = hosts;
    }

    /** {@inheritDoc} */
    @Override public String[] hosts() {
        assert hosts != null;

        return hosts;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Nullable @Override public <T> T innerSplit() {
        return (T)innerSplit;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        boolean writable = innerSplit instanceof Writable;

        out.writeUTF(writable ? innerSplit.getClass().getName() : null);

        if (writable)
            ((Writable)innerSplit).write(out);
        else
            out.writeObject(innerSplit);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        String clsName = in.readUTF();

        if (clsName == null)
            innerSplit = in.readObject();
        else {
            Class<Writable> cls = (Class<Writable>)Class.forName(clsName);

            try {
                innerSplit = U.newInstance(cls);
            }
            catch (GridException e) {
                throw new IOException(e);
            }

            ((Writable)innerSplit).readFields(in);
        }
    }
}
