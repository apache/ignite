package org.apache.calcite.plan.volcano;

import java.util.List;
import java.util.stream.Collectors;

public class VolcanoUtils {
    public static List<RelSubset> otherSubsets(RelSubset subset) {
        return subset.set.subsets.stream()
            .filter(relSubset -> relSubset != subset)
            .collect(Collectors.toList());
    }
}
