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

package org.apache.ignite.ml.structures.partition;

import java.io.Serializable;
import java.util.Iterator;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.LabeledVectorSet;

/**
 * Partition data builder that builds {@link LabeledVectorSet}.
 *
 * @param <K> Type of a key in <tt>upstream</tt> data.
 * @param <V> Type of a value in <tt>upstream</tt> data.
 * @param <C> Type of a partition <tt>context</tt>.
 */
public class LabeledDatasetPartitionDataBuilderOnHeap<K, V, C extends Serializable>
    implements PartitionDataBuilder<K, V, C, LabeledVectorSet<Double, LabeledVector>> {
    /** */
    private static final long serialVersionUID = -7820760153954269227L;

    /** Extractor of X matrix row. */
    private final IgniteBiFunction<K, V, Vector> xExtractor;

    /** Extractor of Y vector value. */
    private final IgniteBiFunction<K, V, Double> yExtractor;

    /**
     * Constructs a new instance of SVM partition data builder.
     *
     * @param xExtractor Extractor of X matrix row.
     * @param yExtractor Extractor of Y vector value.
     */
    public LabeledDatasetPartitionDataBuilderOnHeap(IgniteBiFunction<K, V, Vector> xExtractor,
                                         IgniteBiFunction<K, V, Double> yExtractor) {
        this.xExtractor = xExtractor;
        this.yExtractor = yExtractor;
    }

    /** {@inheritDoc} */
    @Override public LabeledVectorSet<Double, LabeledVector> build(
        LearningEnvironment env,
        Iterator<UpstreamEntry<K, V>> upstreamData,
        long upstreamDataSize, C ctx) {
        int xCols = -1;
        double[][] x = null;
        double[] y = new double[Math.toIntExact(upstreamDataSize)];

        int ptr = 0;

        while (upstreamData.hasNext()) {
            UpstreamEntry<K, V> entry = upstreamData.next();
            Vector row = xExtractor.apply(entry.getKey(), entry.getValue());

            if (xCols < 0) {
                xCols = row.size();
                x = new double[Math.toIntExact(upstreamDataSize)][xCols];
            }
            else
                assert row.size() == xCols : "X extractor must return exactly " + xCols + " columns";

            x[ptr] = row.asArray();

            y[ptr] = yExtractor.apply(entry.getKey(), entry.getValue());

            ptr++;
        }
        return new LabeledVectorSet<>(x, y);
    }
}
