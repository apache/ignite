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
public class GridHadoopSplitWrapper extends GridHadoopInputSplit {
    /** */
    private static final long serialVersionUID = 0L;

    /** Native hadoop input split. */
    private Object innerSplit;

    /** Internal ID */
    private int id;

    /**
     * Creates new split wrapper.
     */
    public GridHadoopSplitWrapper() {
        // No-op.
    }

    /**
     * Creates new split wrapper.
     *
     * @param id Split ID.
     * @param innerSplit Native hadoop input split to wrap or {@code null} if it is serialized in external file.
     * @param hosts Hosts where split is located.
     */
    public GridHadoopSplitWrapper(int id, @Nullable Object innerSplit, String[] hosts) {
        assert hosts != null;

        this.innerSplit = innerSplit;
        this.hosts = hosts;
        this.id = id;
    }

    /**
     * @return Inner split.
     */
    @SuppressWarnings("unchecked")
    public Object innerSplit() {
        return innerSplit;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(id);

        boolean writable = innerSplit instanceof Writable;

        out.writeUTF(writable ? innerSplit.getClass().getName() : null);

        if (writable)
            ((Writable)innerSplit).write(out);
        else
            out.writeObject(innerSplit);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = in.readInt();

        String clsName = in.readUTF();

        if (clsName == null)
            innerSplit = in.readObject();
        else {
            // Split wrapper only used when classes available in our classpath, so Class.forName is ok here.
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

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridHadoopSplitWrapper that = (GridHadoopSplitWrapper)o;

        return id == that.id;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id;
    }
}
