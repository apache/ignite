/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.v2;

import org.gridgain.grid.hadoop.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Split serialized in external file.
 */
public class GridHadoopExternalSplit implements GridHadoopInputSplit {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private long off;

    /** */
    private String[] hosts;

    /**
     * For {@link Externalizable}.
     */
    public GridHadoopExternalSplit() {
        // No-op.
    }

    /**
     * @param hosts Hosts.
     * @param off Offset of this split in external file.
     */
    public GridHadoopExternalSplit(String[] hosts, long off) {
        assert off >= 0 : off;
        assert hosts != null;

        this.hosts = hosts;
        this.off = off;
    }

    /** {@inheritDoc} */
    @Override public String[] hosts() {
        assert hosts != null;

        return hosts;
    }

    /** {@inheritDoc} */
    @Nullable @Override public <T> T innerSplit() {
        throw new IllegalStateException();
    }

    /**
     * @return Offset of this input split in external file.
     */
    public long offset() {
        return off;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(off);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        off = in.readLong();
    }
}
