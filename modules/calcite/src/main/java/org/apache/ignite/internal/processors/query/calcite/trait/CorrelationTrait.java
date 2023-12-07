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

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.ignite.internal.processors.query.calcite.exec.ExecutionContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Correlation trait is created by node that creates and sets correlation variables {@link RelNode#getVariablesSet()}}
 * (e.g. {@link IgniteCorrelatedNestedLoopJoin}) and pass through this trait to input that uses correlation variable(s).
 *
 * The correlation trait is used to prevent the insertion of nodes that cannot preserve correlation in
 * the correlated data stream. e.g. these nodes are IgniteExchange, IgniteTableSpool, etc.
 *
 * Let's describe more details:
 * IgniteExchange: current implementation is not rewindable and not send values of the correlation variables to
 * Senders.
 * (see {@link ExecutionContext#getCorrelated(int)})
 * TableSpool: stores the input data stream and rewind it many times So. it cannot be depends on the value of the
 * correlation variables.
 */
public class CorrelationTrait implements RelTrait {
    /** */
    public static final CorrelationTrait UNCORRELATED = canonize(new CorrelationTrait(Collections.emptyList()));

    /** */
    private final ImmutableSet<CorrelationId> correlations;

    /** */
    public CorrelationTrait(Collection<CorrelationId> correlationIds) {
        correlations = ImmutableSet.copyOf(correlationIds);
    }

    /** */
    public boolean correlated() {
        return !F.isEmpty(correlations);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof CorrelationTrait))
            return false;

        return correlations.equals(((CorrelationTrait)o).correlations);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return correlations.hashCode();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return correlated() ? "correlated" + correlations : "uncorrelated";
    }

    /** {@inheritDoc} */
    @Override public RelTraitDef<CorrelationTrait> getTraitDef() {
        return CorrelationTraitDef.INSTANCE;
    }

    /** {@inheritDoc} */
    @Override public boolean satisfies(RelTrait trait) {
        if (trait == this || this == UNCORRELATED)
            return true;

        if (!(trait instanceof CorrelationTrait))
            return false;

        CorrelationTrait other = (CorrelationTrait)trait;

        return other.correlated() && other.correlationIds().containsAll(correlationIds());
    }

    /** {@inheritDoc} */
    @Override public void register(RelOptPlanner planner) {
        // no-op
    }

    /** */
    private static CorrelationTrait canonize(CorrelationTrait trait) {
        return CorrelationTraitDef.INSTANCE.canonize(trait);
    }

    /** */
    public Set<CorrelationId> correlationIds() {
        return correlations;
    }

    /** */
    public static CorrelationTrait correlations(Collection<CorrelationId> correlationIds) {
        return canonize(new CorrelationTrait(correlationIds));
    }
}
