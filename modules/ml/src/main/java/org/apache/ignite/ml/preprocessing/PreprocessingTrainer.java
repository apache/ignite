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

package org.apache.ignite.ml.preprocessing;

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;

/**
 * Trainer for preprocessor.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 * @param <T> Type of a value returned by base preprocessor.
 * @param <R> Type of a value returned by preprocessor fitted by this trainer.
 */
public interface PreprocessingTrainer<K, V, T, R> {
    /**
     * Fits preprocessor.
     *
     * @param datasetBuilder Dataset builder.
     * @param basePreprocessor Base preprocessor.
     * @return Preprocessor.
     */
    public IgniteBiFunction<K, V, R> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, T> basePreprocessor);

    /**
     * Fits preprocessor.
     *
     * @param ignite Ignite instance.
     * @param cache Ignite cache.
     * @param basePreprocessor Base preprocessor.
     * @return Preprocessor.
     */
    public default IgniteBiFunction<K, V, R> fit(Ignite ignite, IgniteCache<K, V> cache,
        IgniteBiFunction<K, V, T> basePreprocessor) {
        return fit(
            new CacheBasedDatasetBuilder<>(ignite, cache),
            basePreprocessor
        );
    }

    /**
     * Fits preprocessor.
     *
     * @param data Data.
     * @param parts Number of partitions.
     * @param basePreprocessor Base preprocessor.
     * @return Preprocessor.
     */
    public default IgniteBiFunction<K, V, R> fit(Map<K, V> data, int parts,
        IgniteBiFunction<K, V, T> basePreprocessor) {
        return fit(
            new LocalDatasetBuilder<>(data, parts),
            basePreprocessor
        );
    }
}
