/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.common;

import org.gridgain.grid.util.typedef.internal.*;

import static org.gridgain.grid.kernal.ggfs.common.GridGgfsIpcCommand.*;

/**
 * Handshake request.
 */
public class GridGgfsHandshakeRequest extends GridGgfsMessage {
    /** Expected Grid name. */
    private String gridName;

    /** Expected GGFS name. */
    private String ggfsName;

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
     * @return Grid name.
     */
    public String gridName() {
        return gridName;
    }

    /**
     * @param gridName Grid name.
     */
    public void gridName(String gridName) {
        this.gridName = gridName;
    }

    /**
     * @return GGFS name.
     */
    public String ggfsName() {
        return ggfsName;
    }

    /**
     * @param ggfsName GGFS name.
     */
    public void ggfsName(String ggfsName) {
        this.ggfsName = ggfsName;
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridGgfsHandshakeRequest.class, this);
    }
}
