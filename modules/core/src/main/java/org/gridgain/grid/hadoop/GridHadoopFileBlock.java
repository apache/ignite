/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Hadoop file block.
 */
public class GridHadoopFileBlock extends GridHadoopInputSplit {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridToStringInclude
    protected URI file;

    /** */
    @GridToStringInclude
    protected long start;

    /** */
    @GridToStringInclude
    protected long len;

    /**
     * Creates new file block.
     */
    public GridHadoopFileBlock() {
        // No-op.
    }

    /**
     * Creates new file block.
     *
     * @param hosts List of hosts where the block resides.
     * @param file File URI.
     * @param start Start position of the block in the file.
     * @param len Length of the block.
     */
    public GridHadoopFileBlock(String[] hosts, URI file, long start, long len) {
        A.notNull(hosts, "hosts", file, "file");

        this.hosts = hosts;
        this.file = file;
        this.start = start;
        this.len = len;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(file());
        out.writeLong(start());
        out.writeLong(length());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        file = (URI)in.readObject();
        start = in.readLong();
        len = in.readLong();
    }

    /**
     * @return Length.
     */
    public long length() {
        return len;
    }

    /**
     * @param len New length.
     */
    public void length(long len) {
        this.len = len;
    }

    /**
     * @return Start.
     */
    public long start() {
        return start;
    }

    /**
     * @param start New start.
     */
    public void start(long start) {
        this.start = start;
    }

    /**
     * @return File.
     */
    public URI file() {
        return file;
    }

    /**
     * @param file New file.
     */
    public void file(URI file) {
        this.file = file;
    }

    /**
     * @param hosts New hosts.
     */
    public void hosts(String[] hosts) {
        A.notNull(hosts, "hosts");

        this.hosts = hosts;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof GridHadoopFileBlock))
            return false;

        GridHadoopFileBlock that = (GridHadoopFileBlock)o;

        return len == that.len && start == that.start && file.equals(that.file);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = file.hashCode();

        res = 31 * res + (int)(start ^ (start >>> 32));
        res = 31 * res + (int)(len ^ (len >>> 32));

        return res;
    }

    /** {@inheritDoc} */
    public String toString() {
        return S.toString(GridHadoopFileBlock.class, this, "hosts", Arrays.toString(hosts));
    }
}
