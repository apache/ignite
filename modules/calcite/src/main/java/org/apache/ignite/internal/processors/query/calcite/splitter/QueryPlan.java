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

package org.apache.ignite.internal.processors.query.calcite.splitter;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Context;
import org.apache.ignite.internal.processors.query.calcite.rel.CloneContext;

/**
 *
 */
public class QueryPlan {
    private final ImmutableList<Fragment> fragments;

    public QueryPlan(ImmutableList<Fragment> fragments) {
        this.fragments = fragments;
    }

    public void init(Context ctx) {
        for (Fragment fragment : fragments) {
            fragment.init(ctx);
        }
    }

    public ImmutableList<Fragment> fragments() {
        return fragments;
    }

    public QueryPlan clone(CloneContext ctx) {
        ImmutableList.Builder<Fragment> b = ImmutableList.builder();

        for (Fragment f : fragments) {
            b.add(new Fragment(ctx.clone(f.rel)));
        }

        return new QueryPlan(b.build());
    }
}
