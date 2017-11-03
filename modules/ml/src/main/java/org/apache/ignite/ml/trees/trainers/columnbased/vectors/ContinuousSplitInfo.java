package org.apache.ignite.ml.trees.trainers.columnbased.vectors;

import org.apache.ignite.ml.trees.RegionInfo;
import org.apache.ignite.ml.trees.nodes.ContinuousSplitNode;
import org.apache.ignite.ml.trees.nodes.SplitNode;

/**
 * Information about split of continuous region.
 *
 * @param <D> Class encapsulating information about the region.
 */
public class ContinuousSplitInfo<D extends RegionInfo> extends SplitInfo<D> {
    /**
     * Threshold used for split.
     * Samples with values less or equal than this go to left region, others go to the right region.
     */
    private final double threshold;

    /**
     * @param regionIdx Index of region being split.
     * @param threshold Threshold used for split. Samples with values less or equal than this go to left region, others
     * go to the right region.
     * @param leftData Information about left subregion.
     * @param rightData Information about right subregion.
     */
    public ContinuousSplitInfo(int regionIdx, double threshold, D leftData, D rightData) {
        super(regionIdx, leftData, rightData);
        this.threshold = threshold;
    }

    /** {@inheritDoc} */
    @Override public SplitNode createSplitNode(int featureIdx) {
        return new ContinuousSplitNode(threshold, featureIdx);
    }

    /**
     * Threshold used for splits.
     * Samples with values less or equal than this go to left region, others go to the right region.
     */
    public double threshold() {
        return threshold;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "ContinuousSplitInfo [" +
            "threshold=" + threshold +
            ", infoGain=" + infoGain +
            ", regionIdx=" + regionIdx +
            ", leftData=" + leftData +
            ", rightData=" + rightData +
            ']';
    }
}
