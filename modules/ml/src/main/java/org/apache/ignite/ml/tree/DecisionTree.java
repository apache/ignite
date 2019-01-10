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

package org.apache.ignite.ml.tree;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.data.DecisionTreeData;
import org.apache.ignite.ml.tree.data.DecisionTreeDataBuilder;
import org.apache.ignite.ml.tree.impurity.ImpurityMeasure;
import org.apache.ignite.ml.tree.impurity.ImpurityMeasureCalculator;
import org.apache.ignite.ml.tree.impurity.util.StepFunction;
import org.apache.ignite.ml.tree.impurity.util.StepFunctionCompressor;
import org.apache.ignite.ml.tree.leaf.DecisionTreeLeafBuilder;

/**
 * Distributed decision tree trainer that allows to fit trees using row-partitioned dataset.
 *
 * @param <T> Type of impurity measure.
 */
public abstract class DecisionTree<T extends ImpurityMeasure<T>> extends DatasetTrainer<DecisionTreeNode, Double> {
    /** Max tree deep. */
    int maxDeep;

    /** Min impurity decrease. */
    double minImpurityDecrease;

    /** Step function compressor. */
    StepFunctionCompressor<T> compressor;

    /** Decision tree leaf builder. */
    private final DecisionTreeLeafBuilder decisionTreeLeafBuilder;

    /** Use index structure instead of using sorting while learning. */
    protected boolean usingIdx = true;

    /**
     * Constructs a new distributed decision tree trainer.
     *
     * @param maxDeep Max tree deep.
     * @param minImpurityDecrease Min impurity decrease.
     * @param compressor Impurity function compressor.
     * @param decisionTreeLeafBuilder Decision tree leaf builder.
     */
    DecisionTree(int maxDeep, double minImpurityDecrease, StepFunctionCompressor<T> compressor,
        DecisionTreeLeafBuilder decisionTreeLeafBuilder) {
        this.maxDeep = maxDeep;
        this.minImpurityDecrease = minImpurityDecrease;
        this.compressor = compressor;
        this.decisionTreeLeafBuilder = decisionTreeLeafBuilder;
    }

    /** {@inheritDoc} */
    @Override public <K, V> DecisionTreeNode fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {
        try (Dataset<EmptyContext, DecisionTreeData> dataset = datasetBuilder.build(
            envBuilder,
            new EmptyContextBuilder<>(),
            new DecisionTreeDataBuilder<>(featureExtractor, lbExtractor, usingIdx)
        )) {
            return fit(dataset);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Trains new model based on dataset because there is no valid approach to update decision trees.
     *
     * @param mdl Learned model.
     * @param datasetBuilder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return New model based on new dataset.
     */
    @Override protected <K, V> DecisionTreeNode updateModel(DecisionTreeNode mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {

        return fit(datasetBuilder, featureExtractor, lbExtractor);
    }

    /** {@inheritDoc} */
    @Override protected boolean checkState(DecisionTreeNode mdl) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public DecisionTree<T> withEnvironmentBuilder(LearningEnvironmentBuilder envBuilder) {
        return (DecisionTree<T>)super.withEnvironmentBuilder(envBuilder);
    }

    /** */
    public <K,V> DecisionTreeNode fit(Dataset<EmptyContext, DecisionTreeData> dataset) {
        return split(dataset, e -> true, 0, getImpurityMeasureCalculator(dataset));
    }

    /**
     * Returns impurity measure calculator.
     *
     * @param dataset Dataset.
     * @return Impurity measure calculator.
     */
    protected abstract ImpurityMeasureCalculator<T> getImpurityMeasureCalculator(Dataset<EmptyContext, DecisionTreeData> dataset);

    /**
     * Splits the node specified by the given dataset and predicate and returns decision tree node.
     *
     * @param dataset Dataset.
     * @param filter Decision tree node predicate.
     * @param deep Current tree deep.
     * @param impurityCalc Impurity measure calculator.
     * @return Decision tree node.
     */
    private DecisionTreeNode split(Dataset<EmptyContext, DecisionTreeData> dataset, TreeFilter filter, int deep,
        ImpurityMeasureCalculator<T> impurityCalc) {
        if (deep >= maxDeep)
            return decisionTreeLeafBuilder.createLeafNode(dataset, filter);

        StepFunction<T>[] criterionFunctions = calculateImpurityForAllColumns(dataset, filter, impurityCalc, deep);

        if (criterionFunctions == null)
            return decisionTreeLeafBuilder.createLeafNode(dataset, filter);

        SplitPoint splitPnt = calculateBestSplitPoint(criterionFunctions);

        if (splitPnt == null)
            return decisionTreeLeafBuilder.createLeafNode(dataset, filter);

        return new DecisionTreeConditionalNode(
            splitPnt.col,
            splitPnt.threshold,
            split(dataset, updatePredicateForThenNode(filter, splitPnt), deep + 1, impurityCalc),
            split(dataset, updatePredicateForElseNode(filter, splitPnt), deep + 1, impurityCalc),
            null
        );
    }

    /**
     * Calculates impurity measure functions for all columns for the node specified by the given dataset and predicate.
     *
     * @param dataset Dataset.
     * @param filter Decision tree node predicate.
     * @param impurityCalc Impurity measure calculator.
     * @return Array of impurity measure functions for all columns.
     */
    private StepFunction<T>[] calculateImpurityForAllColumns(Dataset<EmptyContext, DecisionTreeData> dataset,
        TreeFilter filter, ImpurityMeasureCalculator<T> impurityCalc, int depth) {

        return dataset.compute(
            part -> {
                if (compressor != null)
                    return compressor.compress(impurityCalc.calculate(part, filter, depth));
                else
                    return impurityCalc.calculate(part, filter, depth);
            }, this::reduce
        );
    }

    /**
     * Calculates best split point.
     *
     * @param criterionFunctions Array of impurity measure functions for all columns.
     * @return Best split point.
     */
    private SplitPoint calculateBestSplitPoint(StepFunction<T>[] criterionFunctions) {
        SplitPoint<T> res = null;

        for (int col = 0; col < criterionFunctions.length; col++) {
            StepFunction<T> criterionFunctionForCol = criterionFunctions[col];

            double[] arguments = criterionFunctionForCol.getX();
            T[] values = criterionFunctionForCol.getY();

            for (int leftSize = 1; leftSize < values.length - 1; leftSize++) {
                if ((values[0].impurity() - values[leftSize].impurity()) > minImpurityDecrease
                    && (res == null || values[leftSize].compareTo(res.val) < 0))
                    res = new SplitPoint<>(values[leftSize], col, calculateThreshold(arguments, leftSize));
            }
        }

        return res;
    }

    /**
     * Merges two arrays gotten from two partitions.
     *
     * @param a First step function.
     * @param b Second step function.
     * @return Merged step function.
     */
    private StepFunction<T>[] reduce(StepFunction<T>[] a, StepFunction<T>[] b) {
        if (a == null)
            return b;
        if (b == null)
            return a;
        else {
            StepFunction<T>[] res = Arrays.copyOf(a, a.length);

            for (int i = 0; i < res.length; i++)
                res[i] = res[i].add(b[i]);

            return res;
        }
    }

    /**
     * Calculates threshold based on the given step function arguments and split point (specified left size).
     *
     * @param arguments Step function arguments.
     * @param leftSize Split point (left size).
     * @return Threshold.
     */
    private double calculateThreshold(double[] arguments, int leftSize) {
        return (arguments[leftSize] + arguments[leftSize + 1]) / 2.0;
    }

    /**
     * Constructs a new predicate for "then" node based on the parent node predicate and split point.
     *
     * @param filter Parent node predicate.
     * @param splitPnt Split point.
     * @return Predicate for "then" node.
     */
    private TreeFilter updatePredicateForThenNode(TreeFilter filter, SplitPoint splitPnt) {
        return filter.and(f -> f[splitPnt.col] > splitPnt.threshold);
    }

    /**
     * Constructs a new predicate for "else" node based on the parent node predicate and split point.
     *
     * @param filter Parent node predicate.
     * @param splitPnt Split point.
     * @return Predicate for "else" node.
     */
    private TreeFilter updatePredicateForElseNode(TreeFilter filter, SplitPoint splitPnt) {
        return filter.and(f -> f[splitPnt.col] <= splitPnt.threshold);
    }

    /**
     * Util class that represents split point.
     */
    private static class SplitPoint<T extends ImpurityMeasure<T>> implements Serializable {
        /** */
        private static final long serialVersionUID = -1758525953544425043L;

        /** Split point impurity measure value. */
        private final T val;

        /** Column. */
        private final int col;

        /** Threshold. */
        private final double threshold;

        /**
         * Constructs a new instance of split point.
         *
         * @param val Split point impurity measure value.
         * @param col Column.
         * @param threshold Threshold.
         */
        SplitPoint(T val, int col, double threshold) {
            this.val = val;
            this.col = col;
            this.threshold = threshold;
        }
    }

    /**
     * Represents DecisionTree as String.
     *
     * @param node Decision tree.
     * @param pretty Use pretty mode.
     */
    public static String printTree(DecisionTreeNode node, boolean pretty) {
        StringBuilder builder = new StringBuilder();
        printTree(node, 0, builder, pretty, false);
        return builder.toString();
    }

    /**
     * Recursive realisation of DecisionTree to String converting.
     *
     * @param node Decision tree.
     * @param depth Current depth.
     * @param builder String builder.
     * @param pretty Use pretty mode.
     */
    private static void printTree(DecisionTreeNode node, int depth, StringBuilder builder, boolean pretty, boolean isThen) {
        builder.append(pretty ? String.join("", Collections.nCopies(depth, "\t")) : "");
        if (node instanceof DecisionTreeLeafNode) {
            DecisionTreeLeafNode leaf = (DecisionTreeLeafNode)node;
            builder.append(String.format("%s return ", isThen ? "then" : "else"))
                .append(String.format("%.4f", leaf.getVal()));
        }
        else if (node instanceof DecisionTreeConditionalNode) {
            DecisionTreeConditionalNode cond = (DecisionTreeConditionalNode)node;
            String prefix = depth == 0 ? "" : (isThen ? "then " : "else ");
            builder.append(String.format("%sif (x", prefix))
                .append(cond.getCol())
                .append(" > ")
                .append(String.format("%.4f", cond.getThreshold()))
                .append(pretty ? ")\n" : ") ");
            printTree(cond.getThenNode(), depth + 1, builder, pretty, true);
            builder.append(pretty ? "\n" : " ");
            printTree(cond.getElseNode(), depth + 1, builder, pretty, false);
        }
        else
            throw new IllegalArgumentException();
    }
}
