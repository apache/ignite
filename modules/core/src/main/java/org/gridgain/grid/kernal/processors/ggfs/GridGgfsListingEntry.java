/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Directory listing entry.
 */
public class GridGgfsListingEntry implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** File id. */
    private GridUuid fileId;

    /** File affinity key. */
    private GridUuid affKey;

    /** Positive block size if file, 0 if directory. */
    private int blockSize;

    /** File length. */
    private long len;

    /** Last access time. */
    private long accessTime;

    /** Last modification time. */
    private long modificationTime;

    /** File properties. */
    private Map<String, String> props;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridGgfsListingEntry() {
        // No-op.
    }

    /**
     * @param fileInfo File info to construct listing entry from.
     */
    public GridGgfsListingEntry(GridGgfsFileInfo fileInfo) {
        fileId = fileInfo.id();
        affKey = fileInfo.affinityKey();

        if (fileInfo.isFile()) {
            blockSize = fileInfo.blockSize();
            len = fileInfo.length();
        }

        props = fileInfo.properties();
        accessTime = fileInfo.accessTime();
        modificationTime = fileInfo.modificationTime();
    }

    /**
     * Creates listing entry with updated length.
     *
     * @param entry Entry.
     * @param len New length.
     */
    public GridGgfsListingEntry(GridGgfsListingEntry entry, long len, long accessTime, long modificationTime) {
        fileId = entry.fileId;
        affKey = entry.affKey;
        blockSize = entry.blockSize;
        props = entry.props;
        this.accessTime = accessTime;
        this.modificationTime = modificationTime;

        this.len = len;
    }

    /**
     * @return Entry file ID.
     */
    public GridUuid fileId() {
        return fileId;
    }

    /**
     * @return File affinity key, if specified.
     */
    public GridUuid affinityKey() {
        return affKey;
    }

    /**
     * @return {@code True} if entry represents file.
     */
    public boolean isFile() {
        return blockSize > 0;
    }

    /**
     * @return {@code True} if entry represents directory.
     */
    public boolean isDirectory() {
        return blockSize == 0;
    }

    /**
     * @return Block size.
     */
    public int blockSize() {
        return blockSize;
    }

    /**
     * @return Length.
     */
    public long length() {
        return len;
    }

    /**
     * @return Last access time.
     */
    public long accessTime() {
        return accessTime;
    }

    /**
     * @return Last modification time.
     */
    public long modificationTime() {
        return modificationTime;
    }

    /**
     * @return Properties map.
     */
    public Map<String, String> properties() {
        return props;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeGridUuid(out, fileId);
        out.writeInt(blockSize);
        out.writeLong(len);
        U.writeStringMap(out, props);
        out.writeLong(accessTime);
        out.writeLong(modificationTime);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fileId = U.readGridUuid(in);
        blockSize = in.readInt();
        len = in.readLong();
        props = U.readStringMap(in);
        accessTime = in.readLong();
        modificationTime = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GridGgfsListingEntry)) return false;

        GridGgfsListingEntry that = (GridGgfsListingEntry)o;

        return fileId.equals(that.fileId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return fileId.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridGgfsListingEntry.class, this);
    }
}
