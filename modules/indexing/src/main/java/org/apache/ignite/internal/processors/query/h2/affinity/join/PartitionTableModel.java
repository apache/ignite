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

package org.apache.ignite.internal.processors.query.h2.affinity.join;

import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Partition join model. Describes how tables are joined with each other.
 */
public class PartitionTableModel {
    /** Join group which could not be applied (e.g. for "ALL" case). */
    public static final int GRP_NONE = -1;

    /** All tables observed during parsing excluding outer. */
    private final Map<String, PartitionJoinTable> tbls = new HashMap<>();

    /** Join groups. */
    private final Map<Integer, PartitionJoinGroup> grps = new HashMap<>();

    /** Talbes which are excluded from partition pruning calculation. */
    private Set<String> excludedTblNames;

    /** Group index generator */
    private int grpIdxGen;

    /**
     * Add table.
     *
     * @param tbl Table.
     * @param aff Affinity descriptor.
     */
    public void addTable(PartitionJoinTable tbl, PartitionJoinAffinityDescriptor aff) {
        int grpIdx = grpIdxGen++;

        tbl.joinGorup(grpIdx);

        tbls.put(tbl.alias(), tbl);
        grps.put(grpIdx, new PartitionJoinGroup(aff).addTable(tbl));
    }

    /**
     * Get table by alias.
     *
     * @param alias Alias.
     * @return Table or {@code null} if it cannot be used for partition pruning.
     */
    @Nullable public PartitionJoinTable table(String alias) {
        PartitionJoinTable res = tbls.get(alias);

        assert res != null || (excludedTblNames != null && excludedTblNames.contains(alias));

        return res;
    }

    /**
     * Add excluded table
     *
     * @param alias Alias.
     */
    public void addExcludedTable(String alias) {
        PartitionJoinTable tbl = tbls.remove(alias);

        if (tbl != null) {
            PartitionJoinGroup grp = grps.get(tbl.joinGroup());

            assert grp != null;

            if (grp.removeTable(tbl))
                grps.remove(tbl.joinGroup());
        }

        if (excludedTblNames == null)
            excludedTblNames = new HashSet<>();

        excludedTblNames.add(alias);
    }

    /**
     * Add equi-join condition. Two joined tables may possibly be merged into a single group.
     *
     * @param cond Condition.
     */
    public void addJoin(PartitionJoinCondition cond) {
        PartitionJoinTable leftTbl = tbls.get(cond.leftAlias());
        PartitionJoinTable rightTbl = tbls.get(cond.rightAlias());

        assert leftTbl != null || (excludedTblNames != null && excludedTblNames.contains(cond.leftAlias()));
        assert rightTbl != null || (excludedTblNames != null && excludedTblNames.contains(cond.rightAlias()));

        // At least one tables is excluded, return.
        if (leftTbl == null || rightTbl == null)
            return;

        // At least one column in condition is not affinity column, return.
        if (!leftTbl.isAffinityColumn(cond.leftColumn()) || !rightTbl.isAffinityColumn(cond.rightColumn()))
            return;

        PartitionJoinGroup leftGrp = grps.get(leftTbl.joinGroup());
        PartitionJoinGroup rightGrp = grps.get(rightTbl.joinGroup());

        assert leftGrp != null;
        assert rightGrp != null;

        // Groups are not compatible, return.
        if (!leftGrp.affinityDescriptor().isCompatible(rightGrp.affinityDescriptor()))
            return;

        // Safe to merge groups.
        for (PartitionJoinTable tbl : rightGrp.tables()) {
            tbl.joinGorup(leftTbl.joinGroup());

            leftGrp.addTable(tbl);
        }

        grps.remove(rightTbl.joinGroup());
    }

    /**
     * Get affinity descriptor for the group.
     *
     * @param grpId Group ID.
     * @return Affinity descriptor or {@code null} if there is no affinity descriptor (e.g. for "NONE" result).
     */
    @Nullable public PartitionJoinAffinityDescriptor joinGroupAffinity(int grpId) {
        if (grpId == GRP_NONE)
            return null;

        PartitionJoinGroup grp = grps.get(grpId);

        assert grp != null;

        return grp.affinityDescriptor();
    }
}
