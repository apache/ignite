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

import java.util.List;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.metadata.MappingService;
import org.apache.ignite.internal.processors.query.calcite.metadata.OptimisticPlanningException;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public abstract class AbstractMultiStepPlan implements MultiStepPlan {
    /** */
    protected final List<Fragment> fragments;

    /** */
    protected final List<GridQueryFieldMetadata> fieldsMeta;

    protected AbstractMultiStepPlan(List<Fragment> fragments, List<GridQueryFieldMetadata> fieldsMeta) {
        this.fragments = fragments;
        this.fieldsMeta = fieldsMeta;
    }

    /** {@inheritDoc} */
    @Override public List<Fragment> fragments() {
        return fragments;
    }

    /** {@inheritDoc} */
    @Override public List<GridQueryFieldMetadata> fieldsMetadata() {
        return fieldsMeta;
    }

    /** {@inheritDoc} */
    @Override public void init(MappingService mappingService, PlanningContext ctx) {
        RelMetadataQuery mq = F.first(fragments).root().getCluster().getMetadataQuery();

        for (int i = 0, j = 0; i < fragments.size();) {
            Fragment fragment = fragments.get(i);

            try {
                fragment.init(mappingService, ctx, mq);

                i++;
            }
            catch (OptimisticPlanningException e) {
                if (++j > 3)
                    throw new IgniteSQLException("Failed to map query.", e);

                replace(fragment, new FragmentSplitter().go(fragment, e.node(), mq));

                i = 0; // restart init routine.
            }
        }
    }

    /** */
    private void replace(Fragment fragment, List<Fragment> replacement) {
        assert !F.isEmpty(replacement);

        fragments.set(fragments.indexOf(fragment), F.first(replacement));
        fragments.addAll(replacement.subList(1, replacement.size()));
    }
}
