// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.communication;

import java.util.*;

/**
 * Listener for messages received from remote nodes.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridMessageListener extends EventListener {
    /**
     * Notification for received messages.
     *
     * @param nodeId ID of node that sent the message. Note that may have already
     *      left topology by the time this message is received.
     * @param msg Message received.
     */
    public void onMessage(UUID nodeId, Object msg);
}
