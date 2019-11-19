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
package org.apache.ignite.internal.processors.query.calcite.rel;

import com.google.common.collect.ImmutableSet;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.util.RelImplementor;

public final class IgniteFilter extends Filter implements IgniteRel {
  private final Set<CorrelationId> variablesSet;

  public IgniteFilter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child,
      RexNode condition, Set<CorrelationId> variablesSet) {
    super(cluster, traitSet, child, condition);
    this.variablesSet = Objects.requireNonNull(variablesSet);
  }

    @Override public Set<CorrelationId> getVariablesSet() {
    return variablesSet;
  }

  @Override public IgniteFilter copy(RelTraitSet traitSet, RelNode input,
      RexNode condition) {
    return new IgniteFilter(getCluster(), traitSet, input, condition, variablesSet);
  }

  /** {@inheritDoc} */
  @Override public <T> T implement(RelImplementor<T> implementor) {
    return implementor.implement(this);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .itemIf("variablesSet", variablesSet, !variablesSet.isEmpty());
  }

  public static IgniteFilter create(Filter filter, RelNode input) {
    RexNode condition = filter.getCondition();
    Set<CorrelationId> variablesSet = filter.getVariablesSet();

    return create(input, condition, variablesSet);
  }

  public static IgniteFilter create(RelNode input, RexNode condition, Set<CorrelationId> variablesSet) {
    RelOptCluster cluster = input.getCluster();
    RelMetadataQuery mq = cluster.getMetadataQuery();

    RelTraitSet traits = cluster.traitSet()
        .replace(IgniteRel.IGNITE_CONVENTION)
        .replaceIf(DistributionTraitDef.INSTANCE, () -> IgniteMdDistribution.filter(mq, input, condition));

    return new IgniteFilter(cluster, traits, input, condition, ImmutableSet.copyOf(variablesSet));
  }
}