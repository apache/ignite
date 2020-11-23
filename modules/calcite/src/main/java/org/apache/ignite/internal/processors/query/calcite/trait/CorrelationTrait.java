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

import com.google.common.collect.ImmutableSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.ignite.internal.util.typedef.F;

/** */
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

        return equals(trait);
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
