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

package org.apache.ignite.mesos;

/**
 * Information about launched task.
 */
public class IgniteTask {
    /** */
    public final String host;

    /** */
    public final double cpuCores;

    /** */
    public final double mem;

    /** */
    public final double disk;

    /**
     * Ignite launched task.
     *
     * @param host Host.
     * @param cpuCores Cpu cores count.
     * @param mem Memory.
     * @param disk Disk.
     */
    public IgniteTask(String host, double cpuCores, double mem, double disk) {
        this.host = host;
        this.cpuCores = cpuCores;
        this.mem = mem;
        this.disk = disk;
    }

    /**
     * @return Host.
     */
    public String host() {
        return host;
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

    /**
     * @return Disk.
     */
    public double disk() {
        return disk;
    }

    @Override public String toString() {
        return "IgniteTask " +
            "host: [" + host + ']' +
            ", cpuCores: [" + cpuCores + "]" +
            ", mem: [" + mem + "]";
    }
}