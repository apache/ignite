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

package org.apache.ignite.ml.structures.newstructures;

import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.apache.ignite.ml.math.exceptions.NoDataException;
import org.apache.ignite.ml.math.exceptions.knn.NoLabelVectorException;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.math.impls.vector.SparseDistributedVector;
import org.apache.ignite.ml.structures.Dataset;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Class for set of labeled vectors.
 */
public class NewLabeledDataset<K, V, L, Row extends LabeledVector> {
    /**
     * Default constructor (required by Externalizable).
     */
    public NewLabeledDataset() {
        super();
    }

    public NewLabeledDataset(DatasetBuilder<K, V> datasetBuilder,
                             IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor, int cols){



    }

    /**
     * Returns label if label is attached or null if label is missed.
     *
     * @param idx Index of observation.
     * @return Label.
     */
    public double label(int idx) {
            return Double.NaN;
    }
}
