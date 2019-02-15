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

package org.apache.ignite.ml.dataset.primitive.builder.data;

import java.io.Serializable;
import java.util.Iterator;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.data.SimpleLabeledDatasetData;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * A partition {@code data} builder that makes {@link SimpleLabeledDatasetData}.
 *
 * @param <K> Type of a key in <tt>upstream</tt> data.
 * @param <V> Type of a value in <tt>upstream</tt> data.
 * @param <C> type of a partition <tt>context</tt>.
 */
public class SimpleLabeledDatasetDataBuilder<K, V, C extends Serializable>
    implements PartitionDataBuilder<K, V, C, SimpleLabeledDatasetData> {
    /** */
    private static final long serialVersionUID = 3678784980215216039L;

    /** Function that extracts features from an {@code upstream} data. */
    private final IgniteBiFunction<K, V, Vector> featureExtractor;

    /** Function that extracts labels from an {@code upstream} data. */
    private final IgniteBiFunction<K, V, double[]> lbExtractor;

    /**
     * Constructs a new instance of partition {@code data} builder that makes {@link SimpleLabeledDatasetData}.
     *
     * @param featureExtractor Function that extracts features from an {@code upstream} data.
     * @param lbExtractor Function that extracts labels from an {@code upstream} data.
     */
    public SimpleLabeledDatasetDataBuilder(IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, double[]> lbExtractor) {
        this.featureExtractor = featureExtractor;
        this.lbExtractor = lbExtractor;
    }

    /** {@inheritDoc} */
    @Override public SimpleLabeledDatasetData build(
        LearningEnvironment env,
        Iterator<UpstreamEntry<K, V>> upstreamData,
        long upstreamDataSize, C ctx) {
        // Prepares the matrix of features in flat column-major format.
        int featureCols = -1;
        int lbCols = -1;
        double[] features = null;
        double[] labels = null;

        int ptr = 0;
        while (upstreamData.hasNext()) {
            UpstreamEntry<K, V> entry = upstreamData.next();

            Vector featureRow = featureExtractor.apply(entry.getKey(), entry.getValue());

            if (featureCols < 0) {
                featureCols = featureRow.size();
                features = new double[Math.toIntExact(upstreamDataSize * featureCols)];
            }
            else
                assert featureRow.size() == featureCols : "Feature extractor must return exactly " + featureCols
                    + " features";

            for (int i = 0; i < featureCols; i++)
                features[Math.toIntExact(i * upstreamDataSize) + ptr] = featureRow.get(i);

            double[] lbRow = lbExtractor.apply(entry.getKey(), entry.getValue());

            if (lbCols < 0) {
                lbCols = lbRow.length;
                labels = new double[Math.toIntExact(upstreamDataSize * lbCols)];
            }

            assert lbRow.length == lbCols : "Label extractor must return exactly " + lbCols + " labels";

            for (int i = 0; i < lbCols; i++)
                labels[Math.toIntExact(i * upstreamDataSize) + ptr] = lbRow[i];

            ptr++;
        }

        return new SimpleLabeledDatasetData(features, labels, Math.toIntExact(upstreamDataSize));
    }
}
