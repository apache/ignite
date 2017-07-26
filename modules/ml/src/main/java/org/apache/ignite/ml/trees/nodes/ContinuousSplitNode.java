package org.apache.ignite.ml.trees.nodes;

import org.apache.ignite.ml.math.Vector;

/**
 * Split node representing split of continuous feature.
 */
public class ContinuousSplitNode extends SplitNode {
    /** Threshold. Values which are less or equal then threshold are assigned to the left subregion. */
    private double threshold;

    /**
     * Construct ContinuousSplitNode by threshold and feature index.
     *
     * @param threshold Threshold.
     * @param featureIdx Feature index.
     */
    public ContinuousSplitNode(double threshold, int featureIdx) {
        super(featureIdx);
        this.threshold = threshold;
    }

    /** {@inheritDoc} */
    @Override public boolean goLeft(Vector v) {
        return v.getX(featureIdx) <= threshold;
    }

    /** Threshold. Values which are less or equal then threshold are assigned to the left subregion. */
    public double threshold() {
        return threshold;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "ContinuousSplitNode{" +
            "threshold=" + threshold +
            '}';
    }
}
