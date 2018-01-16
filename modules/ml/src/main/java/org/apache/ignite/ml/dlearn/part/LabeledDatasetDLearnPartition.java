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

package org.apache.ignite.ml.dlearn.part;

import org.apache.ignite.ml.dlearn.DLearnPartitionStorage;

/**
 * Interface which provides simple dataset API which allows to get or set an underlying feature matrix in flat format
 * and vector of labels.
 */
public class LabeledDatasetDLearnPartition<L> extends DatasetDLeanPartition {
    /** */
    private static final String LABELS_KEY = "labels";

    /** */
    private final DLearnPartitionStorage storage;

    /** */
    public LabeledDatasetDLearnPartition(DLearnPartitionStorage storage) {
        super(storage);
        this.storage = storage;
    }

    /**
     * Sets labels.
     *
     * @param labels labels
     */
    public void setLabels(L[] labels) {
        storage.put(LABELS_KEY, labels);
    }

    /**
     * Retrieves labels.
     *
     * @return labels
     */
    public L[] getLabels() {
        return storage.get(LABELS_KEY);
    }
}
