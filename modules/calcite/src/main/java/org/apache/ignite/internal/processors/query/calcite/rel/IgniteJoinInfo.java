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
import org.apache.calcite.util.ImmutableIntList;

/** */
public class IgniteJoinInfo extends JoinInfo {
    /** Filtered nulls of equi conditions. It usually means presence of IS NOT DISTINCT. */
    private final int matchingNullsCnt;

    /** */
    protected IgniteJoinInfo(
        ImmutableIntList leftKeys,
        ImmutableIntList rightKeys,
        int matchingNullsCnt,
        ImmutableList<RexNode> nonEquis
    ) {
        super(leftKeys, rightKeys, nonEquis);

        this.matchingNullsCnt = matchingNullsCnt;
    }

    /** */
    public static IgniteJoinInfo of(Join join) {
        List<Integer> leftKeys = new ArrayList<>();
        List<Integer> rightKeys = new ArrayList<>();
        List<Boolean> filteredNulls = new ArrayList<>();
        List<RexNode> nonEquis = new ArrayList<>();

        RelOptUtil.splitJoinCondition(join.getLeft(), join.getRight(), join.getCondition(), leftKeys, rightKeys,
            filteredNulls, nonEquis);

        int matchingNullsCnt = 0;

        for (int i = 0; i < filteredNulls.size(); ++i) {
            if (!filteredNulls.get(i))
                ++matchingNullsCnt;
        }

        return new IgniteJoinInfo(
            ImmutableIntList.of(leftKeys.stream().mapToInt(i -> i).toArray()),
            ImmutableIntList.of(rightKeys.stream().mapToInt(i -> i).toArray()),
            matchingNullsCnt,
            ImmutableList.copyOf(nonEquis)
        );
    }

    /** */
    public static IgniteJoinInfo of(ImmutableIntList leftKeys, ImmutableIntList rightKeys) {
        return new IgniteJoinInfo(leftKeys, rightKeys, 0, ImmutableList.of());
    }

    /** */
    public boolean hasMatchingNulls() {
        return matchingNullsCnt != 0;
    }

    /** */
    public int matchingNullsCnt() {
        return matchingNullsCnt;
    }
}
