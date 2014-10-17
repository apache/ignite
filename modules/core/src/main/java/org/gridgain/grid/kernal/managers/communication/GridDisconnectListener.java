/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.communication;

import java.util.*;

/**
 * Node disconnect listener interface.
 */
public interface GridDisconnectListener {
    /**
     * Callback invoked when connection between nodes is closed. This callback invocation does not necessarily
     * mean that node failed or some messages got lost. However, there is such possibility, so listeners can
     * set up timeout objects to check for lost responses.
     *
     * @param nodeId Node ID for which connection was closed.
     */
    public void onNodeDisconnected(UUID nodeId);
}
