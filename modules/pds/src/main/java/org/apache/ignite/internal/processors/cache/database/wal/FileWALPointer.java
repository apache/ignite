package org.apache.ignite.internal.processors.cache.database.wal;

import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * File WAL pointer.
 */
public class FileWALPointer implements WALPointer, Comparable<FileWALPointer> {
    /** */
    private final int idx;

    /** */
    private final int fileOffset;

    /** Written record length */
    private int len;

    /**
     * @param idx File timestamp index.
     * @param fileOffset Offset in file, from the beginning.
     */
    public FileWALPointer(int idx, int fileOffset, int len) {
        this.idx = idx;
        this.fileOffset = fileOffset;
        this.len = len;
    }

    /**
     * @return Timestamp index.
     */
    public int index() {
        return idx;
    }

    /**
     * @return File offset.
     */
    public int fileOffset() {
        return fileOffset;
    }

    /**
     * @return Record length.
     */
    public int length() {
        return len;
    }

    /** {@inheritDoc} */
    @Override public WALPointer next() {
        if (len == 0)
            throw new IllegalStateException("Failed to calculate next WAL pointer " +
                "(this pointer is a terminal): " + this);

        // Return a terminal pointer.
        return new FileWALPointer(idx, fileOffset + len, 0);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof FileWALPointer))
            return false;

        FileWALPointer that = (FileWALPointer)o;

        return idx == that.idx && fileOffset == that.fileOffset;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = idx;

        result = 31 * result + fileOffset;

        return result;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(FileWALPointer o) {
        int res = Integer.compare(idx, o.idx);

        return res == 0 ? Integer.compare(fileOffset, o.fileOffset) : res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FileWALPointer.class, this);
    }
}
