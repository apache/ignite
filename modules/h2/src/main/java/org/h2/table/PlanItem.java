/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import org.h2.index.Index;

/**
 * The plan item describes the index to be used, and the estimated cost when
 * using it.
 */
public class PlanItem {

    /**
     * The cost.
     */
    double cost;

    private int[] masks;
    private Index index;
    private PlanItem joinPlan;
    private PlanItem nestedJoinPlan;

    void setMasks(int[] masks) {
        this.masks = masks;
    }

    int[] getMasks() {
        return masks;
    }

    void setIndex(Index index) {
        this.index = index;
    }

    public Index getIndex() {
        return index;
    }

    PlanItem getJoinPlan() {
        return joinPlan;
    }

    PlanItem getNestedJoinPlan() {
        return nestedJoinPlan;
    }

    void setJoinPlan(PlanItem joinPlan) {
        this.joinPlan = joinPlan;
    }

    void setNestedJoinPlan(PlanItem nestedJoinPlan) {
        this.nestedJoinPlan = nestedJoinPlan;
    }

}
