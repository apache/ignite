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
import org.apache.ignite.ml.dlearn.part.LabeledDatasetDLearnPartition;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/** */
public class LocalLabeledDatasetDLearnPartitionTransformer<V, L>
    extends LocalDatasetDLearnPartitionTransformer<V, LabeledDatasetDLearnPartition<L>> {
    /** */
    private static final long serialVersionUID = -8438445094768312331L;

    /** */
    private final IgniteFunction<V, L> lbExtractor;

    /** */
    public LocalLabeledDatasetDLearnPartitionTransformer(IgniteFunction<V, double[]> featureExtractor, IgniteFunction<V, L> lbExtractor) {
        super(featureExtractor);
        this.lbExtractor = lbExtractor;
    }

    /** */
    @SuppressWarnings("unchecked")
    @Override public void accept(LocalDLearnPartition<V> oldPart,
        LabeledDatasetDLearnPartition<L> newPart) {
        super.accept(oldPart, newPart);
        List<V> partData = oldPart.getPartData();
        L[] labels = (L[]) new Object[partData.size()];
        for (int i = 0; i < partData.size(); i++)
            labels[i] = lbExtractor.apply(partData.get(i));
        newPart.setLabels(labels);
    }
}
