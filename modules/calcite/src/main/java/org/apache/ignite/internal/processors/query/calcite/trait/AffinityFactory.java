/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.trait;

import java.util.List;
import java.util.UUID;
import java.util.function.ToIntFunction;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public final class AffinityFactory extends AbstractDestinationFunctionFactory {
    private final int cacheId;
    private final Object key;

    public AffinityFactory(int cacheId, Object key) {
        this.cacheId = cacheId;
        this.key = key;
    }

    @Override public DestinationFunction create(PlannerContext ctx, NodesMapping mapping, ImmutableIntList keys) {
        assert keys.size() == 1 && mapping != null && !F.isEmpty(mapping.assignments());

        List<List<UUID>> assignments = mapping.assignments();

        if (U.assertionsEnabled()) {
            for (List<UUID> assignment : assignments) {
                assert F.isEmpty(assignment) || assignment.size() == 1;
            }
        }

        ToIntFunction<Object> rowToPart = ctx.kernalContext()
            .cache().context().cacheContext(cacheId).affinity()::partition;

        return row -> assignments.get(rowToPart.applyAsInt(((Object[]) row)[keys.getInt(0)]));
    }

    @Override public Object key() {
        return key;
    }
}
