/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
