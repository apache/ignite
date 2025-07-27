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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;

/** */
public class IgniteJoinInfo extends JoinInfo {
    /** */
    private final Set<Integer> matchingNulls;

    /** */
    protected IgniteJoinInfo(
        ImmutableIntList leftKeys,
        ImmutableIntList rightKeys,
        Set<Integer> matchingNulls,
        ImmutableList<RexNode> nonEquis
    ) {
        super(leftKeys, rightKeys, nonEquis);

        this.matchingNulls = matchingNulls;
    }

    /** */
    public static IgniteJoinInfo of(Join join) {
        List<Integer> leftKeys = new ArrayList<>();
        List<Integer> rightKeys = new ArrayList<>();
        List<Boolean> filteredNulls = new ArrayList<>();
        List<RexNode> nonEquis = new ArrayList<>();

        RelOptUtil.splitJoinCondition(join.getLeft(), join.getRight(), join.getCondition(), leftKeys, rightKeys,
            filteredNulls, nonEquis);

        Set<Integer> bs = Collections.emptySet();

        for (int i = 0; i < filteredNulls.size(); ++i) {
            if (!filteredNulls.get(i)) {
                if (bs == Collections.EMPTY_SET)
                    bs = new HashSet<>();

                bs.add(i);
            }
        }

        return new IgniteJoinInfo(
            ImmutableIntList.of(leftKeys.stream().mapToInt(i -> i).toArray()),
            ImmutableIntList.of(rightKeys.stream().mapToInt(i -> i).toArray()),
            bs,
            ImmutableList.copyOf(nonEquis)
        );
    }

    /** */
    public static IgniteJoinInfo of(ImmutableIntList leftKeys, ImmutableIntList rightKeys) {
        return new IgniteJoinInfo(leftKeys, rightKeys, Collections.emptySet(), ImmutableList.of());
    }

    /** */
    public boolean hasMatchingNulls() {
        return !matchingNulls.isEmpty();
    }

    /** */
    public int matchingNullsCnt() {
        return matchingNulls.size();
    }

    /** */
    public boolean matchingNull(int equiPairIdx) {
        return matchingNulls.contains(equiPairIdx);
    }
}
