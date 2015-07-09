/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.yarn;

import org.apache.hadoop.yarn.api.records.*;

/**
 * Information about launched task.
 */
public class IgniteContainer {
    /** */
    public final ContainerId id;

    /** */
    public final NodeId nodeId;

    /** */
    public final double cpuCores;

    /** */
    public final double mem;

    /**
     * Ignite launched task.
     *
     * @param nodeId Node id.
     * @param cpuCores Cpu cores count.
     * @param mem Memory
     */
    public IgniteContainer(ContainerId id, NodeId nodeId, double cpuCores, double mem) {
        this.id = id;
        this.nodeId = nodeId;
        this.cpuCores = cpuCores;
        this.mem = mem;
    }

    /**
     * @return Id.
     */
    public ContainerId id() {
        return id;
    }

    /**
     * @return Host.
     */
    public NodeId nodeId() {
        return nodeId;
    }

    /**
     * @return Cores count.
     */
    public double cpuCores() {
        return cpuCores;
    }

    /**
     * @return Memory.
     */
    public double mem() {
        return mem;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "IgniteTask [host=" + nodeId.getHost() + ", cpuCores=" + cpuCores + ", mem=" + mem + ']';
    }
}
