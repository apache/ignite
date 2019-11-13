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

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDistribution;
import org.apache.ignite.internal.processors.query.calcite.metadata.RelMetadataQueryEx;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.util.Implementor;

public final class IgniteProject extends Project implements IgniteRel {
  public IgniteProject(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType) {
    super(cluster, traitSet, input, projects, rowType);
  }

  @Override public IgniteProject copy(RelTraitSet traitSet, RelNode input,
      List<RexNode> projects, RelDataType rowType) {
    return new IgniteProject(getCluster(), traitSet, input, projects, rowType);
  }

  /** {@inheritDoc} */
  @Override public <T> T implement(Implementor<T> implementor) {
    return implementor.implement(this);
  }

  public static IgniteProject create(Project project, RelNode input) {
    RelTraitSet traits = project.getTraitSet()
        .replace(IgniteRel.IGNITE_CONVENTION)
        .replaceIf(DistributionTraitDef.INSTANCE, () -> IgniteMdDistribution.project(RelMetadataQueryEx.instance(), input, project.getProjects()));

    return new IgniteProject(project.getCluster(), traits, input, project.getProjects(), project.getRowType());
  }
}
