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

import java.io.ObjectStreamException;
import java.util.List;
import java.util.UUID;
import java.util.function.ToIntFunction;
import org.apache.calcite.plan.Context;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.metadata.NodesMapping;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public final class HashFunctionFactory extends AbstractDestinationFunctionFactory {
    public static final DestinationFunctionFactory INSTANCE = new HashFunctionFactory();

    @Override public DestinationFunction create(Context ctx, NodesMapping m, ImmutableIntList k) {
        assert m != null && !F.isEmpty(m.assignments());

        int[] fields = k.toIntArray();

        ToIntFunction<Object> hashFun = r -> {
            Object[] row = (Object[]) r;

            if (row == null)
                return 0;

            int hash = 1;

            for (int i : fields)
                hash = 31 * hash + (row[i] == null ? 0 : row[i].hashCode());

            return hash;
        };

        List<List<UUID>> assignments = m.assignments();

        if (U.assertionsEnabled()) {
            for (List<UUID> assignment : assignments) {
                assert F.isEmpty(assignment) || assignment.size() == 1;
            }
        }

        return r -> assignments.get(hashFun.applyAsInt(r) % assignments.size());
    }

    @Override public Object key() {
        return "HashFunctionFactory";
    }

    private Object readResolve() throws ObjectStreamException {
        return INSTANCE;
    }
}
