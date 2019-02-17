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

package org.apache.ignite.ml.dataset.primitive;

import java.io.Serializable;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.data.SimpleLabeledDatasetData;

/**
 * A simple labeled dataset introduces additional methods based on a matrix of features and labels vector.
 *
 * @param <C> Type of a partition {@code context}.
 */
public class SimpleLabeledDataset<C extends Serializable> extends DatasetWrapper<C, SimpleLabeledDatasetData> {
    /**
     * Creates a new instance of simple labeled dataset that introduces additional methods based on a matrix of features
     * and labels vector.
     *
     * @param delegate Delegate that performs {@code compute} actions.
     */
    public SimpleLabeledDataset(Dataset<C, SimpleLabeledDatasetData> delegate) {
        super(delegate);
    }
}
