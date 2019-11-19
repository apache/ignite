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
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.util.RelImplementor;

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
  @Override public <T> T implement(RelImplementor<T> implementor) {
    return implementor.implement(this);
  }

  /** Creates a LogicalProject. */
  public static IgniteProject create(final RelNode input, List<? extends RexNode> projects, List<String> fieldNames) {
    final RelOptCluster cluster = input.getCluster();
    final RelDataType rowType =
        RexUtil.createStructType(cluster.getTypeFactory(), projects,
            fieldNames, SqlValidatorUtil.F_SUGGESTER);
    return create(input, projects, rowType);
  }

  public static IgniteProject create(Project project, RelNode input) {
    return create(input, project.getProjects(), project.getRowType());
  }

  public static IgniteProject create(RelNode input, List<? extends RexNode> projects, RelDataType rowType) {
    RelOptCluster cluster = input.getCluster();
    RelMetadataQuery mq = cluster.getMetadataQuery();
    RelTraitSet traits = cluster.traitSet()
        .replace(IgniteRel.IGNITE_CONVENTION)
        .replaceIf(DistributionTraitDef.INSTANCE, () -> IgniteMdDistribution.project(mq, input, projects));

    return new IgniteProject(cluster, traits, input, projects, rowType);
  }
}
