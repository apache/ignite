/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd;

import java.io.*;
import java.util.*;

/**
 * Base class for Visor task arguments intended to query data from a single node.
 */
public class VisorOneNodeArg implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final UUID nodeId;

    /**
     * @param nodeId Node Id.
     */
    public VisorOneNodeArg(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Node Id.
     */
    public UUID nodeId() {
        return nodeId;
    }
}
