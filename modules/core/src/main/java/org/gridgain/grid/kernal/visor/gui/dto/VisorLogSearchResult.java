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
import java.util.*;

/**
 * Result for log search operation.
 * Contains found line and several lines before and after, plus other info.
 */
public class VisorLogSearchResult implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID. */
    private final UUID nid;

    /** File path relative to the search folder. */
    private final String filePath;

    /** File size. */
    private final long fileSize;

    /** Timestamp of last modification of the file. */
    private final long lastModified;

    /** Lines of text including found line and several lines before and after. */
    private final String[] lines;

    /** Line number in the file, 1 based. */
    private final int lineNumber;

    /** Lines count in the file. */
    private final int lineCount;

    /** File content encoding. */
    private final String encoding;

    /**
     * Create log search result with given parameters.
     *
     * @param nid Node ID.
     * @param filePath File path relative to the search folder.
     * @param fileSize File size.
     * @param lastModified Timestamp of last modification of the file.
     * @param lines Lines of text including found line and several lines before and after.
     * @param lineNumber Line number in the file, 1 based.
     * @param lineCount Lines count in the file.
     * @param encoding File content encoding.
     */
    public VisorLogSearchResult(
        UUID nid,
        String filePath,
        long fileSize,
        long lastModified,
        String[] lines,
        int lineNumber,
        int lineCount,
        String encoding
    ) {
        this.nid = nid;
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.lastModified = lastModified;
        this.lines = lines;
        this.lineNumber = lineNumber;
        this.lineCount = lineCount;
        this.encoding = encoding;
    }

    /**
     * @return Node ID.
     */
    public UUID nid() {
        return nid;
    }

    /**
     * @return File path relative to the search folder.
     */
    public String filePath() {
        return filePath;
    }

    /**
     * @return File size.
     */
    public long fileSize() {
        return fileSize;
    }

    /**
     * @return Timestamp of last modification of the file.
     */
    public long lastModified() {
        return lastModified;
    }

    /**
     * @return Lines of text including found line and several lines before and after.
     */
    public String[] lines() {
        return lines;
    }

    /**
     * @return Line number in the file, 1 based.
     */
    public int lineNumber() {
        return lineNumber;
    }

    /**
     * @return Lines count in the file.
     */
    public int lineCount() {
        return lineCount;
    }

    /**
     * @return File content encoding.
     */
    public String encoding() {
        return encoding;
    }

    /**
     * @return Found line.
     */
    public String line() {
        return lines[lines.length / 2];
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorLogSearchResult.class, this);
    }
}
