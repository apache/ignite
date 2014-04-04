/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.loadtests.dsi;

import java.io.*;
import java.util.*;

/**
 *
 */
public class GridDsiMessage implements Serializable {
    /** Terminal ID. */
    private String terminalId;

    /** Node ID. */
    private UUID nodeId;

    /**
     * Message constructor.
     *
     * @param terminalId Terminal ID.
     * @param nodeId Node ID.
     */
    public GridDsiMessage(String terminalId, UUID nodeId) {
        this.terminalId = terminalId;
        this.nodeId = nodeId;
    }

    /**
     * @return Terminal ID.
     */
    public String getTerminalId() {
        return terminalId;
    }

    /**
     * Sets terminal ID.
     * @param terminalId Terminal ID.
     */
    public void setTerminalId(String terminalId) {
        this.terminalId = terminalId;
    }

    /**
     * @return Node ID.
     */
    public UUID getNodeId() {
        return nodeId;
    }

    /**
     * Sets node ID.
     *
     * @param nodeId Node ID.
     */
    public void setNodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }
}
