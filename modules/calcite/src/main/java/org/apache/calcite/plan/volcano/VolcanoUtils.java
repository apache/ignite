package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.RelOptCost;

public class VolcanoUtils {
    /** */
    public static RelOptCost bestCost(RelSubset relSubset) {
        return relSubset.bestCost;
    }
}
