package org.apache.ignite.ml.trees.trainers.columnbased.vectors;

import org.apache.ignite.ml.trees.RegionInfo;
import org.apache.ignite.ml.trees.nodes.ContinuousSplitNode;
import org.apache.ignite.ml.trees.nodes.SplitNode;

public class ContinuousSplitInfo<D extends RegionInfo> extends SplitInfo<D> {
    private double threshold;

    public ContinuousSplitInfo(int intervalIdx, double threshold, D leftData, D rightData) {
        super(intervalIdx, leftData, rightData);
        this.threshold = threshold;
    }

    @Override public SplitNode createSplitNode(int featureIdx) {
        return new ContinuousSplitNode(threshold, featureIdx);
    }

    public double threshold() {
        return threshold;
    }
}
