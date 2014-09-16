/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.dto;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Represents block of bytes from a file, could be optionally zipped.
 */
public class VisorFileBlock implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** File path. */
    private final String path;

    /** Marker position. */
    private final long offset;

    /** File size. */
    private final long size;

    /** Timestamp of last modification of the file. */
    private final long lastModified;

    /** Whether data was zipped. */
    private final boolean zipped;

    /** Data bytes. */
    private final byte[] data;

    /**
     * Create file block with given parameters.
     *
     * @param path File path.
     * @param offset Marker position.
     * @param size File size.
     * @param lastModified Timestamp of last modification of the file.
     * @param zipped Whether data was zipped.
     * @param data Data bytes.
     */
    public VisorFileBlock(String path, long offset, long size, long lastModified, boolean zipped, byte[] data) {
        this.path = path;
        this.offset = offset;
        this.size = size;
        this.lastModified = lastModified;
        this.zipped = zipped;
        this.data = data;
    }

    /**
     * @return File path.
     */
    public String path() {
        return path;
    }

    /**
     * @return Marker position.
     */
    public long offset() {
        return offset;
    }

    /**
     * @return File size.
     */
    public long size() {
        return size;
    }

    /**
     * @return Timestamp of last modification of the file.
     */
    public long lastModified() {
        return lastModified;
    }

    /**
     * @return Whether data was zipped.
     */
    public boolean zipped() {
        return zipped;
    }

    /**
     * @return Data bytes.
     */
    public byte[] data() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorFileBlock.class, this);
    }
}
