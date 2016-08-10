package org.apache.ignite.hadoop.fs;

import java.util.Collections;
import java.util.Map;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of the IfgsFile interface for the local filesystem.
 */
public class LocalFileSystemIgfsFile implements IgfsFile {
    private final IgfsPath path;
    private final boolean isFile;
    private final boolean isDirectory;
    private final int blockSize;
    private final long modificationTime;
    private final long length;
    private final Map<String, String> props;

    /**
     * @param path IGFS path.
     * @param isFile Path is a file.
     * @param isDirectory Path is a directory.
     * @param blockSize Block size in bytes.
     * @param modificationTime Modification time in millis.
     * @param length File length in bytes.
     * @param props Properties.
     */
    public LocalFileSystemIgfsFile(IgfsPath path, boolean isFile, boolean isDirectory, int blockSize,
        long modificationTime, long length, Map<String, String> props) {
        this.path = path;
        this.isFile = isFile;
        this.isDirectory = isDirectory;
        this.blockSize = blockSize;
        this.modificationTime = modificationTime;
        this.length = length;
        this.props = props;
    }

    /** {@inheritDoc} */
    @Override public IgfsPath path() {
        return path;
    }

    /** {@inheritDoc} */
    @Override public boolean isFile() {
        return isFile;
    }

    /** {@inheritDoc} */
    @Override public boolean isDirectory() {
        return isDirectory;
    }

    /** {@inheritDoc} */
    @Override public int blockSize() {
        return blockSize;
    }

    /** {@inheritDoc} */
    @Override public long groupBlockSize() {
        return blockSize();
    }

    /** {@inheritDoc} */
    @Override public long accessTime() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public long modificationTime() {
        return modificationTime;
    }

    /** {@inheritDoc} */
    @Override public String property(String name) throws IllegalArgumentException {
        return props.get(name);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String property(String name, @Nullable String dfltVal) {
        String res = props.get(name);
        if (res == null)
            return dfltVal;
        else
            return res;
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> properties() {
        return props;
    }

    /** {@inheritDoc} */
    @Override public long length() {
        return length;
    }
}
