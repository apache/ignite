/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.communication;

import org.gridgain.grid.*;

import java.util.*;

/**
 * Process descriptor used to identify process for which task is running.
 */
public class GridHadoopProcessDescriptor {
    /** Parent node ID. */
    private UUID parentNodeId;

    /** Process ID. */
    private UUID procId;

    /** Address. */
    private String addr;

    /** TCP port. */
    private int tcpPort;

    /** Shared memory port. */
    private int shmemPort;

    /**
     * @param procId Process ID.
     */
    public GridHadoopProcessDescriptor(UUID parentNodeId, UUID procId) {
        this.procId = procId;
    }

    /**
     * Gets process ID.
     *
     * @return Process ID.
     */
    public UUID processId() {
        return procId;
    }

    /**
     * Gets parent node ID.
     *
     * @return Parent node ID.
     */
    public UUID parentNodeId() {
        return parentNodeId;
    }

    public String address() {
        return addr;
    }

    /**
     * @return Shared memory port.
     */
    public int sharedMemoryPort() {
        return shmemPort;
    }

    /**
     * @return TCP port.
     */
    public int tcpPort() {
        return tcpPort;
    }
}
