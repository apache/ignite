/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.hadoop.taskexecutor.external;

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Process descriptor used to identify process for which task is running.
 */
public class HadoopProcessDescriptor implements Serializable {
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
    public HadoopProcessDescriptor(UUID parentNodeId, UUID procId) {
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

        if (!(o instanceof HadoopProcessDescriptor))
            return false;

        HadoopProcessDescriptor that = (HadoopProcessDescriptor)o;

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
        return S.toString(HadoopProcessDescriptor.class, this);
    }
}