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
    private L firstClsLbl;

    /**
     * Second class lbl.
     */
    private L secondClsLbl;

    /**
     * Creates an instance of BinaryClassificationEvaluationContext.
     */
    public BinaryClassificationEvaluationContext() {
        this.firstClsLbl = null;
        this.secondClsLbl = null;
    }

    /**
     * Creates an instance of BinaryClassificationEvaluationContext.
     *
     * @param firstClsLbl  First class lbl.
     * @param secondClsLbl Second class lbl.
     */
    public BinaryClassificationEvaluationContext(L firstClsLbl, L secondClsLbl) {
        this.firstClsLbl = firstClsLbl;
        this.secondClsLbl = secondClsLbl;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void aggregate(LabeledVector<L> vector) {
        L lb = vector.label();
        if (firstClsLbl == null)
            this.firstClsLbl = lb;
        else if (secondClsLbl == null && !lb.equals(firstClsLbl)) {
            secondClsLbl = lb;
            swapLabelsIfNeed();
        }
        else
            checkNewLabel(lb);
    }

    /**
     * Swaps labels in sorting order if it's possible.
     */
    private void swapLabelsIfNeed() {
        if (firstClsLbl instanceof Comparable) {
            Comparable cmp1 = (Comparable)firstClsLbl;
            int res = cmp1.compareTo(secondClsLbl);
            if (res > 0) {
                L tmp = secondClsLbl;
                secondClsLbl = firstClsLbl;
                firstClsLbl = tmp;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override public BinaryClassificationEvaluationContext<L> mergeWith(
        BinaryClassificationEvaluationContext<L> other) {
        checkNewLabel(other.firstClsLbl);
        checkNewLabel(other.secondClsLbl);

        List<L> uniqLabels = new ArrayList<>(4);
        uniqLabels.add(this.firstClsLbl);
        uniqLabels.add(this.secondClsLbl);
        uniqLabels.add(other.firstClsLbl);
        uniqLabels.add(other.secondClsLbl);
        Stream<L> s = uniqLabels.stream().filter(Objects::nonNull).distinct();
        if (firstClsLbl instanceof Comparable || secondClsLbl instanceof Comparable ||
            other.firstClsLbl instanceof Comparable || other.secondClsLbl instanceof Comparable)
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
    public L getFirstClsLbl() {
        return firstClsLbl;
    }

    /**
     * Returns second class label.
     *
     * @return Second class label.
     */
    public L getSecondClsLbl() {
        return secondClsLbl;
    }

    /**
     * Checks new label to merge-ability with this context.
     *
     * @param lb Label.
     */
    private void checkNewLabel(L lb) {
        A.ensure(
            firstClsLbl == null || secondClsLbl == null || lb == null ||
                lb.equals(firstClsLbl) || lb.equals(secondClsLbl),
            "Unable to collect binary classification ctx stat. There are more than two labels. " +
                "First label = " + firstClsLbl +
                ", second label = " + secondClsLbl +
                ", another label = " + lb
        );
    }
}
