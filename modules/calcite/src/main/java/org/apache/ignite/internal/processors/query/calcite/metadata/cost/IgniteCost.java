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

package org.apache.ignite.internal.processors.query.calcite.metadata.cost;

import java.util.Objects;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.tostring.S;

/**
 * Estimated execution cost of a {@link RelNode}. Measured in abstract points.
 */
public class IgniteCost implements RelOptCost {
    /** Cost of a passing a single row through an execution node. */
    public static final double ROW_PASS_THROUGH_COST = 1;

    /** Size of a particular field. */
    public static final double AVERAGE_FIELD_SIZE = 4; // such accuracy should be enough for an estimate

    /** Cost of a comparison of one row. */
    public static final double ROW_COMPARISON_COST = 3;

    /** Memory cost of a aggregate call. */
    public static final double AGG_CALL_MEM_COST = 5;

    /** Cost of a lookup at the hash. */
    public static final double HASH_LOOKUP_COST = 10;

    /**
     * With broadcast distribution each row will be sent to the each distination node, thus the total bytes amount will be multiplies of the
     * destination nodes count. Right now it's just a const.
     */
    public static final double BROADCAST_DISTRIBUTION_PENALTY = 5;

    /**
     *
     */
    static final IgniteCost ZERO = new IgniteCost(0, 0, 0, 0, 0);

    /**
     *
     */
    static final IgniteCost TINY = new IgniteCost(1, 1, 1, 1, 1);

    /**
     *
     */
    static final IgniteCost HUGE = new IgniteCost(
            Double.MAX_VALUE,
            Double.MAX_VALUE,
            Double.MAX_VALUE,
            Double.MAX_VALUE,
            Double.MAX_VALUE
    );

    /**
     *
     */
    static final IgniteCost INFINITY = new IgniteCost(
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            Double.POSITIVE_INFINITY
    );

    /** Count of the processed rows. */
    private final double rowCount;

    /** Amount of CPU points. */
    private final double cpu;

    /** Amount of Memory points. */
    private final double memory;

    /** Amount of IO points. */
    private final double io;

    /** Amount of Network points. */
    private final double network;

    /**
     * @param rowCount Row count.
     * @param cpu      Cpu.
     * @param memory   Memory.
     * @param io       Io.
     * @param network  Network.
     */
    IgniteCost(double rowCount, double cpu, double memory, double io, double network) {
        this.rowCount = rowCount;
        this.cpu = cpu;
        this.memory = memory;
        this.io = io;
        this.network = network;
    }

    /** {@inheritDoc} */
    @Override
    public double getRows() {
        return rowCount;
    }

    /** {@inheritDoc} */
    @Override
    public double getCpu() {
        return cpu;
    }

    /** {@inheritDoc} */
    @Override
    public double getIo() {
        return io;
    }

    /**
     * @return Usage of Memory resources.
     */
    public double getMemory() {
        return memory;
    }

    /**
     * @return Usage of Network resources.
     */
    public double getNetwork() {
        return network;
    }

    /** {@inheritDoc} */
    @Override
    public boolean isInfinite() {
        return this == INFINITY
                || rowCount == Double.POSITIVE_INFINITY
                || cpu == Double.POSITIVE_INFINITY
                || memory == Double.POSITIVE_INFINITY
                || io == Double.POSITIVE_INFINITY
                || network == Double.POSITIVE_INFINITY;
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(rowCount, cpu, io, memory, network);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("FloatingPointEquality")
    @Override
    public boolean equals(RelOptCost cost) {
        return this == cost
                || (cost instanceof IgniteCost
                && rowCount == ((IgniteCost) cost).rowCount
                && cpu == ((IgniteCost) cost).cpu
                && memory == ((IgniteCost) cost).memory
                && io == ((IgniteCost) cost).io
                && network == ((IgniteCost) cost).network);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isEqWithEpsilon(RelOptCost cost) {
        return this == cost
                || (cost instanceof IgniteCost
                && Math.abs(rowCount - ((IgniteCost) cost).rowCount) < RelOptUtil.EPSILON
                && Math.abs(cpu - ((IgniteCost) cost).cpu) < RelOptUtil.EPSILON
                && Math.abs(memory - ((IgniteCost) cost).memory) < RelOptUtil.EPSILON
                && Math.abs(io - ((IgniteCost) cost).io) < RelOptUtil.EPSILON
                && Math.abs(network - ((IgniteCost) cost).network) < RelOptUtil.EPSILON);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isLe(RelOptCost cost) {
        IgniteCost other = (IgniteCost) cost;

        return this == cost || (cpu + memory + io + network) <= (other.cpu + other.memory + other.io + other.network);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isLt(RelOptCost cost) {
        IgniteCost other = (IgniteCost) cost;

        return this != cost && (cpu + memory + io + network) < (other.cpu + other.memory + other.io + other.network);
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost plus(RelOptCost cost) {
        IgniteCost other = (IgniteCost) cost;

        return new IgniteCost(
                rowCount + other.rowCount,
                cpu + other.cpu,
                memory + other.memory,
                io + other.io,
                network + other.network
        );
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost minus(RelOptCost cost) {
        IgniteCost other = (IgniteCost) cost;

        return new IgniteCost(
                rowCount - other.rowCount,
                cpu - other.cpu,
                memory - other.memory,
                io - other.io,
                network - other.network
        );
    }

    /** {@inheritDoc} */
    @Override
    public RelOptCost multiplyBy(double factor) {
        return new IgniteCost(
                rowCount * factor,
                cpu * factor,
                memory * factor,
                io * factor,
                network * factor
        );
    }

    /** {@inheritDoc} */
    @Override
    public double divideBy(RelOptCost cost) {
        throw new UnsupportedOperationException(IgniteCost.class.getSimpleName() + "#divideBy");
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(IgniteCost.class, this);
    }
}
