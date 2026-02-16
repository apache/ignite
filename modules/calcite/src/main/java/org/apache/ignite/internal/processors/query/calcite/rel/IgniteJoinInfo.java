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

package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.ArrayList;
import java.util.List;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.util.typedef.F;

/** Extended {@link JoinInfo}. */
public class IgniteJoinInfo extends JoinInfo {
    /** Conditions with mathing nulls. It usually means presence of 'IS DISTINCT' / 'IS NOT DISTINCT'. */
    private final ImmutableBitSet allowNulls;

    /** */
    public IgniteJoinInfo(
        ImmutableIntList leftKeys,
        ImmutableIntList rightKeys,
        ImmutableBitSet allowNulls,
        ImmutableList<RexNode> nonEquis
    ) {
        super(leftKeys, rightKeys, nonEquis);

        this.allowNulls = allowNulls;
    }

    /** */
    public static IgniteJoinInfo of(Join join) {
        List<Integer> leftKeys = new ArrayList<>();
        List<Integer> rightKeys = new ArrayList<>();
        List<Boolean> skipNulls = new ArrayList<>();
        List<RexNode> nonEquis = new ArrayList<>();

        RelOptUtil.splitJoinCondition(join.getLeft(), join.getRight(), join.getCondition(), leftKeys, rightKeys,
            skipNulls, nonEquis);

        ImmutableBitSet.Builder allowNulls = null;

        if (!F.isEmpty(skipNulls)) {
            allowNulls = ImmutableBitSet.builder();

            for (int i = 0; i < skipNulls.size(); ++i) {
                if (!skipNulls.get(i))
                    allowNulls.set(i);
            }
        }

        return new IgniteJoinInfo(
            ImmutableIntList.copyOf(leftKeys),
            ImmutableIntList.copyOf(rightKeys),
            allowNulls == null ? ImmutableBitSet.of() : allowNulls.build(),
            ImmutableList.copyOf(nonEquis)
        );
    }

    /** */
    public ImmutableBitSet allowNulls() {
        return allowNulls;
    }
}
