/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.lang.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * File or directory information.
 */
public final class GridGgfsFileImpl implements GridGgfsFile, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Path to this file. */
    private GridGgfsPath path;

    /** File id. */
    private IgniteUuid fileId;

    /** Block size. */
    private int blockSize;

    /** Group block size. */
    private long grpBlockSize;

    /** File length. */
    private long len;

    /** Last access time. */
    private long accessTime;

    /** Last modification time. */
    private long modificationTime;

    /** Properties. */
    private Map<String, String> props;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridGgfsFileImpl() {
        // No-op.
    }

    /**
     * Constructs directory info.
     *
     * @param path Path.
     */
    public GridGgfsFileImpl(GridGgfsPath path, GridGgfsFileInfo info, long globalGrpBlockSize) {
        A.notNull(path, "path");
        A.notNull(info, "info");

        this.path = path;
        fileId = info.id();

        if (info.isFile()) {
            blockSize = info.blockSize();
            len = info.length();

            grpBlockSize = info.affinityKey() == null ? globalGrpBlockSize :
                info.length() == 0 ? globalGrpBlockSize : info.length();
        }

        props = info.properties();

        if (props == null)
            props = Collections.emptyMap();

        accessTime = info.accessTime();
        modificationTime = info.modificationTime();
    }

    /**
     * Constructs file instance.
     *
     * @param path Path.
     * @param entry Listing entry.
     */
    public GridGgfsFileImpl(GridGgfsPath path, GridGgfsListingEntry entry, long globalGrpSize) {
        A.notNull(path, "path");
        A.notNull(entry, "entry");

        this.path = path;
        fileId = entry.fileId();

        blockSize = entry.blockSize();

        grpBlockSize = entry.affinityKey() == null ? globalGrpSize :
            entry.length() == 0 ? globalGrpSize : entry.length();

        len = entry.length();
        props = entry.properties();

        accessTime = entry.accessTime();
        modificationTime = entry.modificationTime();
    }

    /** {@inheritDoc} */
    @Override public GridGgfsPath path() {
        return path;
    }

    /**
     * @return File ID.
     */
    public IgniteUuid fileId() {
        return fileId;
    }

    /** {@inheritDoc} */
    @Override public boolean isFile() {
        return blockSize > 0;
    }

    /** {@inheritDoc} */
    @Override public boolean isDirectory() {
        return blockSize == 0;
    }

    /** {@inheritDoc} */
    @Override public long length() {
        return len;
    }

    /** {@inheritDoc} */
    @Override public int blockSize() {
        return blockSize;
    }

    /** {@inheritDoc} */
    @Override public long groupBlockSize() {
        return grpBlockSize;
    }

    /** {@inheritDoc} */
    @Override public long accessTime() {
        return accessTime;
    }

    /** {@inheritDoc} */
    @Override public long modificationTime() {
        return modificationTime;
    }

    /** {@inheritDoc} */
    @Override public String property(String name) throws IllegalArgumentException {
        String val = props.get(name);

        if (val ==  null)
            throw new IllegalArgumentException("File property not found [path=" + path + ", name=" + name + ']');

        return val;
    }

    /** {@inheritDoc} */
    @Override public String property(String name, @Nullable String dfltVal) {
        String val = props.get(name);

        return val == null ? dfltVal : val;
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> properties() {
        return props;
    }

    /**
     * Writes object to data output.
     *
     * @param out Data output.
     */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        path.writeExternal(out);

        out.writeInt(blockSize);
        out.writeLong(grpBlockSize);
        out.writeLong(len);
        U.writeStringMap(out, props);
        out.writeLong(accessTime);
        out.writeLong(modificationTime);
    }

    /**
     * Reads object from data input.
     *
     * @param in Data input.
     */
    @Override public void readExternal(ObjectInput in) throws IOException {
        path = new GridGgfsPath();

        path.readExternal(in);

        blockSize = in.readInt();
        grpBlockSize = in.readLong();
        len = in.readLong();
        props = U.readStringMap(in);
        accessTime = in.readLong();
        modificationTime = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return path.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o == this)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GridGgfsFileImpl that = (GridGgfsFileImpl)o;

        return path.equals(that.path);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridGgfsFileImpl.class, this);
    }
}
