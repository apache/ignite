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

package org.apache.ignite.ml.preprocessing.maxabsscaling;

import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;

/**
 * Trainer of the maxabsscaling preprocessor.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class MaxAbsScalerTrainer<K, V> implements PreprocessingTrainer<K, V, Vector, Vector> {
    /** {@inheritDoc} */
    @Override public MaxAbsScalerPreprocessor<K, V> fit(
        LearningEnvironmentBuilder envBuilder,
        DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> basePreprocessor) {
        try (Dataset<EmptyContext, MaxAbsScalerPartitionData> dataset = datasetBuilder.build(
            envBuilder,
            (env, upstream, upstreamSize) -> new EmptyContext(),
            (env, upstream, upstreamSize, ctx) -> {
                double[] maxAbs = null;

                while (upstream.hasNext()) {
                    UpstreamEntry<K, V> entity = upstream.next();
                    Vector row = basePreprocessor.apply(entity.getKey(), entity.getValue());

                    if (maxAbs == null) {
                        maxAbs = new double[row.size()];
                        for (int i = 0; i < maxAbs.length; i++)
                            maxAbs[i] = .0;
                    }
                    else
                        assert maxAbs.length == row.size() : "Base preprocessor must return exactly " + maxAbs.length
                            + " features";

                    for (int i = 0; i < row.size(); i++) {
                        if (Math.abs(row.get(i)) > Math.abs(maxAbs[i]))
                            maxAbs[i] = Math.abs(row.get(i));
                    }
                }
                return new MaxAbsScalerPartitionData(maxAbs);
            }
        )) {
            double[] maxAbs = dataset.compute(MaxAbsScalerPartitionData::getMaxAbs,
                (a, b) -> {
                    if (a == null)
                        return b;

                    if (b == null)
                        return a;

                    double[] res = new double[a.length];

                    for (int i = 0; i < res.length; i++)
                        res[i] = Math.max(Math.abs(a[i]), Math.abs(b[i]));

                    return res;
                });
            return new MaxAbsScalerPreprocessor<>(maxAbs, basePreprocessor);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
