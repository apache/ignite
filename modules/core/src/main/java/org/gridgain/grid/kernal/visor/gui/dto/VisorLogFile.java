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
 * Visor log file.
 */
public class VisorLogFile implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** File path. */
    private final String path;

    /** File size. */
    private final long size;

    /** File last modified timestamp. */
    private final long lastModified;

    /**
     * Create log file for given file.
     *
     * @param file Log file.
     */
    public VisorLogFile(File file) {
        this(file.getAbsolutePath(), file.length(), file.lastModified());
    }

    /**
     * Create log file with given parameters.
     *
     * @param path File path.
     * @param size File size.
     * @param lastModified File last modified date.
     */
    public VisorLogFile(String path, long size, long lastModified) {
        this.path = path;
        this.size = size;
        this.lastModified = lastModified;
    }

    /**
     * @return File path.
     */
    public String path() {
        return path;
    }

    /**
     * @return File size.
     */
    public long size() {
        return size;
    }

    /**
     * @return File last modified timestamp.
     */
    public long lastModified() {
        return lastModified;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorLogFile.class, this);
    }
}
