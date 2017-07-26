package org.apache.ignite.ml.trees.nodes;

import org.apache.ignite.ml.math.Vector;

/**
 * Terminal node of the decision tree.
 */
public class Leaf implements DecisionTreeNode {
    /**
     * Value in subregion represented by this node.
     */
    private double val;

    /**
     * Construct the leaf of decision tree.
     *
     * @param val Value in subregion represented by this node.
     */
    public Leaf(double val) {
        this.val = val;
    }

    /**
     * Return value in subregion represented by this node.
     *
     * @param v Vector.
     * @return Value in subregion represented by this node.
     */
    @Override public double process(Vector v) {
        return val;
    }
}
