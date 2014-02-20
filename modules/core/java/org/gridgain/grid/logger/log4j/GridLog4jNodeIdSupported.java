// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.logger.log4j;

import java.util.*;

/**
 * Interface for GridGain file appenders to attach node ID to log file names.
 *
 * @author @java.author
 * @version @java.version
 */
public interface GridLog4jNodeIdSupported {
    /**
     * Sets node ID.
     *
     * @param nodeId Node ID.
     */
    public void setNodeId(UUID nodeId);

    /**
     * Gets node ID.
     *
     * @return Node ID.
     */
    public UUID getNodeId();
}
