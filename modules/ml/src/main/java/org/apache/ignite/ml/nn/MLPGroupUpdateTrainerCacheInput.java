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

package org.apache.ignite.ml.nn;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.nn.initializers.MLPInitializer;
import org.apache.ignite.ml.nn.trainers.distributed.AbstractMLPGroupUpdateTrainerInput;
import org.apache.ignite.ml.nn.trainers.distributed.MLPGroupUpdateTrainer;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.Utils;

/**
 * Input for {@link MLPGroupUpdateTrainer} where batches are taken from cache of labeled vectors.
 */
public class MLPGroupUpdateTrainerCacheInput extends AbstractMLPGroupUpdateTrainerInput {
    /**
     * Cache of labeled vectors.
     */
    private final IgniteCache<Integer, LabeledVector<Vector, Vector>> cache;

    /**
     * Size of batch to return on each training iteration.
     */
    private final int batchSize;

    /**
     * Multilayer perceptron.
     */
    private final MultilayerPerceptron mlp;

    /**
     * Random number generator.
     */
    private Random rand;

    /**
     * Construct instance of this class with given parameters.
     *
     * @param arch Architecture of multilayer perceptron.
     * @param init Initializer of multilayer perceptron.
     * @param networksCnt Count of networks to be trained in parallel by {@link MLPGroupUpdateTrainer}.
     * @param cache Cache with labeled vectors.
     * @param batchSize Size of batch to return on each training iteration.
     * @param rand RNG.
     */
    public MLPGroupUpdateTrainerCacheInput(MLPArchitecture arch, MLPInitializer init,
        int networksCnt, IgniteCache<Integer, LabeledVector<Vector, Vector>> cache,
        int batchSize, Random rand) {
        super(networksCnt);

        this.batchSize = batchSize;
        this.cache = cache;
        this.mlp = new MultilayerPerceptron(arch, init);
    }

    /**
     * Construct instance of this class with given parameters.
     *
     * @param arch Architecture of multilayer perceptron.
     * @param init Initializer of multilayer perceptron.
     * @param networksCnt Count of networks to be trained in parallel by {@link MLPGroupUpdateTrainer}.
     * @param cache Cache with labeled vectors.
     * @param batchSize Size of batch to return on each training iteration.
     */
    public MLPGroupUpdateTrainerCacheInput(MLPArchitecture arch, MLPInitializer init,
        int networksCnt, IgniteCache<Integer, LabeledVector<Vector, Vector>> cache,
        int batchSize) {
        this(arch, init, networksCnt, cache, batchSize, new Random());
    }

        /**
         * Construct instance of this class with given parameters and default initializer.
         *
         * @param arch Architecture of multilayer perceptron.
         * @param networksCnt Count of networks to be trained in parallel by {@link MLPGroupUpdateTrainer}.
         * @param cache Cache with labeled vectors.
         * @param batchSize Size of batch to return on each training iteration.
         */
    public MLPGroupUpdateTrainerCacheInput(MLPArchitecture arch, int networksCnt,
        IgniteCache<Integer, LabeledVector<Vector, Vector>> cache,
        int batchSize) {
        this(arch, null, networksCnt, cache, batchSize);
    }

    /** {@inheritDoc} */
    @Override public IgniteSupplier<IgniteBiTuple<Matrix, Matrix>> batchSupplier() {
        String cName = cache.getName();
        int bs = batchSize;
        Random r = rand;

        return () -> {
            Ignite ignite = Ignition.localIgnite();
            IgniteCache<Integer, LabeledVector<Vector, Vector>> cache = ignite.getOrCreateCache(cName);
            int total = cache.size();
            Affinity<Integer> affinity = ignite.affinity(cName);

            List<Integer> allKeys = IntStream.range(0, total).boxed().collect(Collectors.toList());
            List<Integer> keys = new ArrayList<>(affinity.mapKeysToNodes(allKeys).get(ignite.cluster().localNode()));

            int locKeysCnt = keys.size();

            int[] selected = Utils.selectKDistinct(locKeysCnt, Math.min(bs, locKeysCnt), r);

            // Get dimensions of vectors in cache. We suppose that every feature vector has
            // same dimension d 1 and every label has the same dimension d2.
            LabeledVector<Vector, Vector> dimEntry = cache.get(keys.get(selected[0]));

            Matrix inputs = new DenseLocalOnHeapMatrix(dimEntry.features().size(), bs);
            Matrix groundTruth = new DenseLocalOnHeapMatrix(dimEntry.label().size(), bs);

            for (int i = 0; i < selected.length; i++) {
                LabeledVector<Vector, Vector> labeled = cache.get(selected[i]);

                inputs.assignColumn(i, labeled.features());
                groundTruth.assignColumn(i, labeled.label());
            }

            return new IgniteBiTuple<>(inputs, groundTruth);
        };
    }

    /** {@inheritDoc} */
    @Override public MultilayerPerceptron mdl() {
        return mlp;
    }
}
