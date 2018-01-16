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

package org.apache.ignite.ml.dlearn.context.transformer.local;

import java.util.List;
import org.apache.ignite.ml.dlearn.context.local.LocalDLearnPartition;
import org.apache.ignite.ml.dlearn.part.DatasetDLeanPartition;
import org.apache.ignite.ml.math.functions.IgniteBiConsumer;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/** */
public class LocalDatasetDLearnPartitionTransformer<V, P extends DatasetDLeanPartition>
    implements IgniteBiConsumer<LocalDLearnPartition<V>, P> {
    /** */
    private static final long serialVersionUID = -7567051002880704559L;

    /** */
    private final IgniteFunction<V, double[]> featureExtractor;

    /** */
    public LocalDatasetDLearnPartitionTransformer(IgniteFunction<V, double[]> featureExtractor) {
        this.featureExtractor = featureExtractor;
    }

    /** */
    @Override public void accept(LocalDLearnPartition<V> oldPart, P newPart) {
        List<V> partData = oldPart.getPartData();
        if (partData != null && !partData.isEmpty()) {
            double[] features = null;
            int m = partData.size(), n = 0;
            for (int i = 0; i < partData.size(); i++) {
                double[] rowFeatures = featureExtractor.apply(partData.get(i));

                if (i == 0) {
                    n = rowFeatures.length;
                    features = new double[m * n];
                }

                if (rowFeatures.length != n)
                    throw new IllegalStateException();

                for (int j = 0; j < rowFeatures.length; j++)
                    features[j * m + i] = rowFeatures[j];
            }
            newPart.setFeatures(features);
            newPart.setRows(m);
        }
    }
}