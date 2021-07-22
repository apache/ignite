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

package org.apache.ignite.internal.processors.query.calcite.trait;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityService;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationGroup;

/**
 * Ignite distribution trait.
 */
public interface IgniteDistribution extends RelDistribution {
    /**
     * @return Distribution function.
     */
    DistributionFunction function();

    /**
     * Creates a destination based on this function algorithm, given nodes mapping and distribution keys.
     *
     * @param ectx Execution context.
     * @param affinityService Affinity function source.
     * @param targetGroup Target mapping.
     * @return Destination function.
     */
    <Row> Destination<Row> destination(ExecutionContext<Row> ectx, AffinityService affinityService,
        ColocationGroup targetGroup);

    /** {@inheritDoc} */
    @Override ImmutableIntList getKeys();

    /** {@inheritDoc} */
    @Override IgniteDistribution apply(Mappings.TargetMapping mapping);
}
