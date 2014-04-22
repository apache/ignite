/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.hadoop;

import java.io.*;
import java.net.*;

/**
 * Hadoop file block.
 */
public class GridHadoopFileBlock implements Externalizable {
    /** */
    protected String[] hosts;

    /** */
    protected URI file;

    /** */
    protected long start;

    /** */
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
        this.hosts = hosts;
        this.file = file;
        this.start = start;
        this.len = len;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(hosts());
        out.writeObject(file());
        out.writeLong(start());
        out.writeLong(length());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        hosts = (String[])in.readObject();
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
     * @return Hosts.
     */
    public String[] hosts() {
        return hosts;
    }

    /**
     * @param hosts New hosts.
     */
    public void hosts(String[] hosts) {
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
        int result = file.hashCode();

        result = 31 * result + (int)(start ^ (start >>> 32));
        result = 31 * result + (int)(len ^ (len >>> 32));

        return result;
    }
}
