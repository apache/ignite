/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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