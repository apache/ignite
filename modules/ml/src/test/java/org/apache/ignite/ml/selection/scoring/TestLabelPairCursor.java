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

package org.apache.ignite.ml.selection.scoring;

import java.util.Iterator;
import java.util.List;
import org.apache.ignite.ml.selection.scoring.cursor.LabelPairCursor;
import org.jetbrains.annotations.NotNull;

/**
 * Util truth with prediction cursor to be used in tests.
 *
 * @param <L> Type of a label (truth or prediction).
 */
public class TestLabelPairCursor<L> implements LabelPairCursor<L> {
    /** List of truth values. */
    private final List<L> truth;

    /** List of predicted values. */
    private final List<L> predicted;

    /**
     * Constructs a new instance of test truth with prediction cursor.
     *
     * @param truth List of truth values.
     * @param predicted List of predicted values.
     */
    public TestLabelPairCursor(List<L> truth, List<L> predicted) {
        this.truth = truth;
        this.predicted = predicted;
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        /* Do nothing. */
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<LabelPair<L>> iterator() {
        return new TestTruthWithPredictionIterator<>(truth.iterator(), predicted.iterator());
    }

    /**
     * Util truth with prediction iterator to be used in tests.
     *
     * @param <L> Type of a label (truth or prediction).
     */
    private static final class TestTruthWithPredictionIterator<L> implements Iterator<LabelPair<L>> {
        /** Iterator of truth values. */
        private final Iterator<L> truthIter;

        /** Iterator of predicted values. */
        private final Iterator<L> predictedIter;

        /**
         * Constructs a new instance of test truth with prediction iterator.
         *
         * @param truthIter Iterator of truth values.
         * @param predictedIter Iterator of predicted values.
         */
        public TestTruthWithPredictionIterator(Iterator<L> truthIter, Iterator<L> predictedIter) {
            this.truthIter = truthIter;
            this.predictedIter = predictedIter;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return truthIter.hasNext() && predictedIter.hasNext();
        }

        /** {@inheritDoc} */
        @Override public LabelPair<L> next() {
            return new LabelPair<>(truthIter.next(), predictedIter.next());
        }
    }
}
