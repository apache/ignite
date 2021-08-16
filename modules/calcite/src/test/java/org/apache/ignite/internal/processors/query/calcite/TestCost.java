/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite;

/**
 * Test cost with nulls for unknown values.
 */
public class TestCost {
    /** */
    Double rowCount;

    /** */
    Double cpu;

    /** */
    Double memory;

    /** */
    Double io;

    /** */
    Double network;

    /**
     * @return Row count.
     */
    public Double rowCount() {
        return rowCount;
    }

    /**
     * @return Cpu.
     */
    public Double cpu() {
        return cpu;
    }

    /**
     * @return Memory
     */
    public Double memory() {
        return memory;
    }

    /**
     * @return Io.
     */
    public Double io() {
        return io;
    }

    /**
     * @return Network.
     */
    public Double network() {
        return network;
    }

    /**
     * Constructor.
     *
     * @param rowCount Row count.
     * @param cpu Cpu.
     * @param memory Memory.
     * @param io Io.
     * @param network Network.
     */
    public TestCost(Double rowCount, Double cpu, Double memory, Double io, Double network) {
        this.rowCount = rowCount;
        this.cpu = cpu;
        this.memory = memory;
        this.io = io;
        this.network = network;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "TestCost{" +
            "rowCount=" + rowCount +
            ", cpu=" + cpu +
            ", memory=" + memory +
            ", io=" + io +
            ", network=" + network +
            '}';
    }
}
