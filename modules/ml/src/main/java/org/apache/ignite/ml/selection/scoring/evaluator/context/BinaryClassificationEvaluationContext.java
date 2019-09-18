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

package org.apache.ignite.ml.selection.scoring.evaluator.context;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * This context tries to define positive and negative labels for estimation of binary classifier.
 */
public class BinaryClassificationEvaluationContext<L extends Serializable> implements EvaluationContext<L, BinaryClassificationEvaluationContext<L>> {
    /**
     * Serial version uid.
     */
    private static final long serialVersionUID = 658785331349096576L;

    /**
     * First class lbl.
     */
    private L firstClassLbl;

    /**
     * Second class lbl.
     */
    private L secondClassLbl;

    /**
     * Creates an instance of BinaryClassificationEvaluationContext.
     */
    public BinaryClassificationEvaluationContext() {
        this.firstClassLbl = null;
        this.secondClassLbl = null;
    }

    /**
     * Creates an instance of BinaryClassificationEvaluationContext.
     *
     * @param firstClassLbl  First class lbl.
     * @param secondClassLbl Second class lbl.
     */
    public BinaryClassificationEvaluationContext(L firstClassLbl, L secondClassLbl) {
        this.firstClassLbl = firstClassLbl;
        this.secondClassLbl = secondClassLbl;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void aggregate(LabeledVector<L> vector) {
        L label = vector.label();
        if (firstClassLbl == null)
            this.firstClassLbl = label;
        else if (secondClassLbl == null && !label.equals(firstClassLbl)) {
            secondClassLbl = label;
            swapLabelsIfNeed();
        }
        else
            checkNewLabel(label);
    }

    /**
     * Swaps labels in sorting order if it's possible.
     */
    private void swapLabelsIfNeed() {
        if (firstClassLbl instanceof Comparable) {
            Comparable cmp1 = (Comparable)firstClassLbl;
            int res = cmp1.compareTo(secondClassLbl);
            if (res > 0) {
                L tmp = secondClassLbl;
                secondClassLbl = firstClassLbl;
                firstClassLbl = tmp;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override public BinaryClassificationEvaluationContext<L> mergeWith(
        BinaryClassificationEvaluationContext<L> other) {
        checkNewLabel(other.firstClassLbl);
        checkNewLabel(other.secondClassLbl);

        List<L> uniqLabels = new ArrayList<>(4);
        uniqLabels.add(this.firstClassLbl);
        uniqLabels.add(this.secondClassLbl);
        uniqLabels.add(other.firstClassLbl);
        uniqLabels.add(other.secondClassLbl);
        Stream<L> s = uniqLabels.stream().filter(Objects::nonNull).distinct();
        if (firstClassLbl instanceof Comparable || secondClassLbl instanceof Comparable ||
            other.firstClassLbl instanceof Comparable || other.secondClassLbl instanceof Comparable)
            s = s.sorted();
        uniqLabels = s.collect(Collectors.toList());

        A.ensure(uniqLabels.size() < 3, "labels.length < 3");
        return new BinaryClassificationEvaluationContext<>(
            uniqLabels.isEmpty() ? null : uniqLabels.get(0),
            uniqLabels.size() < 2 ? null : uniqLabels.get(1)
        );
    }

    /**
     * Returns first class label.
     *
     * @return First class label.
     */
    public L getFirstClassLbl() {
        return firstClassLbl;
    }

    /**
     * Returns second class label.
     *
     * @return Second class label.
     */
    public L getSecondClassLbl() {
        return secondClassLbl;
    }

    /**
     * Checks new label to mergeability with this context.
     *
     * @param label Label.
     */
    private void checkNewLabel(L label) {
        A.ensure(
            firstClassLbl == null || secondClassLbl == null || label == null ||
                label.equals(firstClassLbl) || label.equals(secondClassLbl),
            "Unable to collect binary classification ctx stat. There are more than two labels. " +
                "First label = " + firstClassLbl +
                ", second label = " + secondClassLbl +
                ", another label = " + label
        );
    }
}
