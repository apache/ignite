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

package org.apache.ignite.ml.selection.scoring.cursor;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.selection.scoring.LabelPair;
import org.jetbrains.annotations.NotNull;

/**
 * Truth with prediction cursor based on a locally stored data.
 *
 * @param <L> Type of a label (truth or prediction).
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class LocalLabelPairCursor<L, K, V, T> implements LabelPairCursor<L> {
    /** Map with {@code upstream} data. */
    private final Map<K, V> upstreamMap;

    /** Filter for {@code upstream} data. */
    private final IgniteBiPredicate<K, V> filter;

    /** Feature extractor. */
    private final IgniteBiFunction<K, V, Vector> featureExtractor;

    /** Label extractor. */
    private final IgniteBiFunction<K, V, L> lbExtractor;

    /** Model for inference. */
    private final Model<Vector, L> mdl;

    /**
     * Constructs a new instance of local truth with prediction cursor.
     *
     * @param upstreamMap Map with {@code upstream} data.
     * @param filter Filter for {@code upstream} data.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param mdl Model for inference.
     */
    public LocalLabelPairCursor(Map<K, V> upstreamMap, IgniteBiPredicate<K, V> filter,
                                IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor,
                                Model<Vector, L> mdl) {
        this.upstreamMap = upstreamMap;
        this.filter = filter;
        this.featureExtractor = featureExtractor;
        this.lbExtractor = lbExtractor;
        this.mdl = mdl;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        /* Do nothing. */
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<LabelPair<L>> iterator() {
        return new TruthWithPredictionIterator(upstreamMap.entrySet().iterator());
    }

    /**
     * Util iterator that filters map entries and makes predictions using the model.
     */
    private class TruthWithPredictionIterator implements Iterator<LabelPair<L>> {
        /** Base iterator. */
        private final Iterator<Map.Entry<K, V>> iter;

        /** Next found entry. */
        private Map.Entry<K, V> nextEntry;

        /**
         * Constructs a new instance of truth with prediction iterator.
         *
         * @param iter Base iterator.
         */
        public TruthWithPredictionIterator(Iterator<Map.Entry<K, V>> iter) {
            this.iter = iter;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            findNext();

            return nextEntry != null;
        }

        /** {@inheritDoc} */
        @Override public LabelPair<L> next() {
            if (!hasNext())
                throw new NoSuchElementException();

            K key = nextEntry.getKey();
            V val = nextEntry.getValue();

            Vector features = featureExtractor.apply(key, val);
            L lb = lbExtractor.apply(key, val);

            nextEntry = null;

            return new LabelPair<>(lb, mdl.apply(features));
        }

        /**
         * Finds next entry using the specified filter.
         */
        private void findNext() {
            while (nextEntry == null && iter.hasNext()) {
                Map.Entry<K, V> entry = iter.next();

                if (filter.apply(entry.getKey(), entry.getValue())) {
                    this.nextEntry = entry;
                    break;
                }
            }
        }
    }
}
