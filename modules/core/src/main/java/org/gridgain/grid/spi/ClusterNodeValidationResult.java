/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi;

import java.util.*;

/**
 * Result of joining node validation.
 */
public class ClusterNodeValidationResult {
    /** Offending node ID. */
    private final UUID nodeId;

    /** Error message to be logged locally. */
    private final String msg;

    /** Error message to be sent to joining node. */
    private final String sndMsg;

    /**
     * @param nodeId Offending node ID.
     * @param msg Message logged locally.
     * @param sndMsg Message sent to joining node.
     */
    public ClusterNodeValidationResult(UUID nodeId, String msg, String sndMsg) {
        this.nodeId = nodeId;
        this.msg = msg;
        this.sndMsg = sndMsg;
    }

    /**
     * @return Offending node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Message to be logged locally.
     */
    public String message() {
        return msg;
    }

    /**
     * @return Message to be sent to joining node.
     */
    public String sendMessage() {
        return sndMsg;
    }
}
