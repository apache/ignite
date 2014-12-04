// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.plugin;

import org.apache.ignite.*;
import org.gridgain.grid.*;

import java.util.*;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class PluginValidationException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Offending node ID. */
    private UUID nodeId;

    /** Error message to send to the offending node. */
    private String rmtMsg;

    /**
     * Constructs invalid plugin exception.
     *
     * @param msg Local error message.
     * @param rmtMsg Error message to send to the offending node.
     * @param nodeId ID of the offending node.
     */
    public PluginValidationException(String msg, String rmtMsg, UUID nodeId) {
        super(msg);

        this.nodeId = nodeId;
        this.rmtMsg = rmtMsg;
    }


    /**
     * @return Offending node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Message to be sent to joining node.
     */
    public String remoteMessage() {
        return rmtMsg;
    }
}
