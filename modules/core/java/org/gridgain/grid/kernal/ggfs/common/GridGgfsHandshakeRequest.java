// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.common;

import org.gridgain.grid.kernal.processors.ggfs.*;

import static org.gridgain.grid.kernal.processors.ggfs.GridGgfsIpcCommand.*;

/**
 * Handshake request.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridGgfsHandshakeRequest extends GridGgfsMessage {
    /** Logger directory. */
    private String logDir;

    /** {@inheritDoc} */
    @Override public GridGgfsIpcCommand command() {
        return HANDSHAKE;
    }

    /** {@inheritDoc} */
    @Override public void command(GridGgfsIpcCommand cmd) {
        // No-op.
    }

    /**
     * @return Log directory.
     */
    public String logDirectory() {
        return logDir;
    }

    /**
     * @param logDir Log directory.
     */
    public void logDirectory(String logDir) {
        this.logDir = logDir;
    }
}
