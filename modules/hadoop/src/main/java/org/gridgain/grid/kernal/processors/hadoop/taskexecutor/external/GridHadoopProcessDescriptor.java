/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Process descriptor used to identify process for which task is running.
 */
public class GridHadoopProcessDescriptor implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

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
     * @param parentNodeId Parent node ID.
     * @param procId Process ID.
     */
    public GridHadoopProcessDescriptor(UUID parentNodeId, UUID procId) {
        this.parentNodeId = parentNodeId;
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

    /**
     * Gets host address.
     *
     * @return Host address.
     */
    public String address() {
        return addr;
    }

    /**
     * Sets host address.
     *
     * @param addr Host address.
     */
    public void address(String addr) {
        this.addr = addr;
    }

    /**
     * @return Shared memory port.
     */
    public int sharedMemoryPort() {
        return shmemPort;
    }

    /**
     * Sets shared memory port.
     *
     * @param shmemPort Shared memory port.
     */
    public void sharedMemoryPort(int shmemPort) {
        this.shmemPort = shmemPort;
    }

    /**
     * @return TCP port.
     */
    public int tcpPort() {
        return tcpPort;
    }

    /**
     * Sets TCP port.
     *
     * @param tcpPort TCP port.
     */
    public void tcpPort(int tcpPort) {
        this.tcpPort = tcpPort;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof GridHadoopProcessDescriptor))
            return false;

        GridHadoopProcessDescriptor that = (GridHadoopProcessDescriptor)o;

        return parentNodeId.equals(that.parentNodeId) && procId.equals(that.procId);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = parentNodeId.hashCode();

        result = 31 * result + procId.hashCode();

        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridHadoopProcessDescriptor.class, this);
    }
}
