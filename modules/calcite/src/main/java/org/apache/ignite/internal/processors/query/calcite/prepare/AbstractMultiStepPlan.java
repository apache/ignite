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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import com.google.common.primitives.Ints;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.exec.partition.PartitionNode;
import org.apache.ignite.internal.processors.query.calcite.exec.partition.PartitionPruningContext;
import org.apache.ignite.internal.processors.query.calcite.metadata.AffinityService;
import org.apache.ignite.internal.processors.query.calcite.metadata.ColocationMappingException;
import org.apache.ignite.internal.processors.query.calcite.metadata.FragmentMappingException;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public abstract class AbstractMultiStepPlan extends AbstractQueryPlan implements MultiStepPlan {
    /** */
    protected final FieldsMetadata fieldsMetadata;

    /** */
    protected final FieldsMetadata paramsMetadata;

    /** */
    protected final QueryTemplate queryTemplate;

    /** */
    private final String textPlan;

    /** */
    protected AbstractMultiStepPlan(
        String qry,
        String textPlan,
        QueryTemplate queryTemplate,
        FieldsMetadata fieldsMetadata,
        @Nullable FieldsMetadata paramsMetadata
    ) {
        super(qry);

        this.textPlan = textPlan;
        this.queryTemplate = queryTemplate;
        this.fieldsMetadata = fieldsMetadata;
        this.paramsMetadata = paramsMetadata;
    }

    /** {@inheritDoc} */
    @Override public FieldsMetadata fieldsMetadata() {
        return fieldsMetadata;
    }

    /** {@inheritDoc} */
    @Override public FieldsMetadata paramsMetadata() {
        return paramsMetadata;
    }

    /** {@inheritDoc} */
    @Override public ExecutionPlan init(
        MappingService mappingService,
        AffinityService affSvc,
        MappingQueryContext mapCtx
    ) {
        ExecutionPlan executionPlan0 = queryTemplate.map(mappingService, mapCtx);

        if (F.isEmpty(executionPlan0.fragments()))
            return executionPlan0;

        if (!F.isEmpty(mapCtx.partitions())) {
            List<Fragment> fragments = executionPlan0.fragments();

            fragments = Commons.transform(fragments, f -> {
                try {
                    return f.filterByPartitions(mapCtx.partitions());
                }
                catch (ColocationMappingException e) {
                    throw new FragmentMappingException("Failed to calculate physical distribution", f, f.root(), e);
                }
            });

            return new ExecutionPlan(executionPlan0.topologyVersion(), fragments, executionPlan0.partitionNodes());
        }
        else if (!mapCtx.isLocal() && mapCtx.unwrap(BaseQueryContext.class) != null) {
            BaseQueryContext qryCtx = mapCtx.unwrap(BaseQueryContext.class);

            List<Fragment> fragments = executionPlan0.fragments();
            List<PartitionNode> partNodes = executionPlan0.partitionNodes();

            fragments = Commons.transform(Pair.zip(fragments, partNodes), pair -> {
                Fragment fragment = pair.left;
                PartitionNode partNode = pair.right;

                Collection<Integer> parts0 = partNode.apply(new PartitionPruningContext(affSvc,
                        new BaseDataContext(qryCtx.typeFactory()), mapCtx.queryParameters()));

                if (parts0 == null)
                    return fragment;

                int[] parts = !parts0.isEmpty() ? Ints.toArray(parts0) : U.EMPTY_INTS;
                if (parts.length > 1)
                    Arrays.sort(parts);

                try {
                    return fragment.filterByPartitions(parts);
                }
                catch (ColocationMappingException e) {
                    throw new FragmentMappingException("Failed to calculate physical distribution", fragment, fragment.root(), e);
                }
            });

            return new ExecutionPlan(executionPlan0.topologyVersion(), fragments, partNodes);
        }

        return executionPlan0;
    }

    /** {@inheritDoc} */
    @Override public String textPlan() {
        return textPlan;
    }
}
