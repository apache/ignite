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

package org.apache.ignite.internal.processors.query.calcite.rule;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDerivedDistribution;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDistribution;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * Ignite Project converter.
 */
public class ProjectConverter extends IgniteConverter {
    public static final ConverterRule INSTANCE = new ProjectConverter();

    /**
     * Creates a converter.
     */
    public ProjectConverter() {
        super(LogicalProject.class, "ProjectConverter");
    }

    /** {@inheritDoc} */
    @Override protected List<RelNode> convert0(RelNode rel) {
        LogicalProject project = (LogicalProject) rel;

        RelNode input = convert(project.getInput(), IgniteConvention.INSTANCE);

        RelOptCluster cluster = rel.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();

        List<IgniteDistribution> distrs = IgniteMdDerivedDistribution.deriveDistributions(input, IgniteConvention.INSTANCE, mq);

        return Commons.transform(distrs, d -> create(project, input, d));
    }

    /** */
    private static IgniteProject create(LogicalProject project, RelNode input, IgniteDistribution distr) {
        RelTraitSet traits = project.getTraitSet()
            .replace(IgniteMdDistribution.project(input.getRowType(), distr, project.getProjects()))
            .replace(IgniteConvention.INSTANCE);

        return new IgniteProject(project.getCluster(), traits, convert(input, distr), project.getProjects(), project.getRowType());
    }
}
