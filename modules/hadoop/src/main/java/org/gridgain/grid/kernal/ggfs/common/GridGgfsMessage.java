/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.ggfs.common;

/**
 * Abstract class for all messages sent between GGFS client (Hadoop File System implementation) and
 * GGFS server (GridGain data node).
 */
public abstract class GridGgfsMessage {
    /** GGFS command. */
    private GridGgfsIpcCommand cmd;

    /**
     * @return Command.
     */
    public GridGgfsIpcCommand command() {
        return cmd;
    }

    /**
     * @param cmd Command.
     */
    public void command(GridGgfsIpcCommand cmd) {
        this.cmd = cmd;
    }
}
