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

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;

/**
 * Ignite cost factory.
 * Creates a desired cost with regards to the weight of each particular resource.
 */
public class IgniteCostFactory implements RelOptCostFactory {
    /** Cpu weight. */
    private final double cpuWeight;

    /** Memory weight. */
    private final double memoryWeight;

    /** Io weight. */
    private final double ioWeight;

    /** Network weight. */
    private final double networkWeight;

    /**
     * Creates a factory with default weights equal to 1.
     */
    public IgniteCostFactory() {
        cpuWeight = 1;
        memoryWeight = 1;
        ioWeight = 1;
        networkWeight = 1;
    }

    /**
     * Cerates a factory with provided weights. Each weight should be non negative value.
     */
    public IgniteCostFactory(double cpuWeight, double memoryWeight, double ioWeight, double networkWeight) {
        if (cpuWeight < 0 || memoryWeight < 0 || ioWeight < 0 || networkWeight < 0)
            throw new IllegalArgumentException("Weight should be non negative: cpu=" + cpuWeight +
                ", memory=" + memoryWeight + ", io=" + ioWeight + ", network=" + networkWeight);

        this.cpuWeight = cpuWeight;
        this.memoryWeight = memoryWeight;
        this.ioWeight = ioWeight;
        this.networkWeight = networkWeight;
    }

    /** {@inheritDoc} */
    @Override public RelOptCost makeCost(double rowCount, double cpu, double io) {
        return makeCost(rowCount, cpu, io, 0, 0);
    }

    /**
     * Creates a cost object with regards to resources' weight.
     *
     * @param rowCount Count of processed rows.
     * @param cpu Amount of consumed CPU.
     * @param io Amount of consumed Io.
     * @param memory Amount of consumed Memory.
     * @param network Amount of consumed Network.
     *
     * @return Cost object.
     */
    public RelOptCost makeCost(double rowCount, double cpu, double io, double memory, double network) {
        return new IgniteCost(rowCount, cpu * cpuWeight, memory * memoryWeight, io * ioWeight, network * networkWeight);
    }

    /** {@inheritDoc} */
    @Override public RelOptCost makeHugeCost() {
        return IgniteCost.HUGE;
    }

    /** {@inheritDoc} */
    @Override public RelOptCost makeInfiniteCost() {
        return IgniteCost.INFINITY;
    }

    /** {@inheritDoc} */
    @Override public RelOptCost makeTinyCost() {
        return IgniteCost.TINY;
    }

    /** {@inheritDoc} */
    @Override public RelOptCost makeZeroCost() {
        return IgniteCost.ZERO;
    }
}
