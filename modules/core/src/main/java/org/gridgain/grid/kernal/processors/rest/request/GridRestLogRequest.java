/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.request;

import org.gridgain.grid.util.typedef.internal.*;

/**
 * Grid command request of log file.
 */
public class GridRestLogRequest extends GridRestRequest {
    /** Task name. */
    private String path;

    /** From line, inclusive, indexing from 0. */
    private int from = -1;

    /** To line, inclusive, indexing from 0, can exceed count of lines in log. */
    private int to = -1;

    /**
     * @return Path to log file.
     */
    public String path() {
        return path;
    }

    /**
     * @param path Path to log file.
     */
    public void path(String path) {
        this.path = path;
    }

    /**
     * @return From line, inclusive, indexing from 0.
     */
    public int from() {
        return from;
    }

    /**
     * @param from From line, inclusive, indexing from 0.
     */
    public void from(int from) {
        this.from = from;
    }

    /**
     * @return To line, inclusive, indexing from 0.
     */
    public int to() {
        return to;
    }

    /**
     * @param to To line, inclusive, indexing from 0.
     */
    public void to(int to) {
        this.to = to;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridRestLogRequest.class, this, super.toString());
    }
}
