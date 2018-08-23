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

package org.apache.ignite.ml.dataset.impl.bagging;

import org.apache.ignite.ml.math.functions.IgniteConsumer;

public class BaggedDatasetPartition implements AutoCloseable {
    private BaggedVector[] dataset;

    public BaggedDatasetPartition(BaggedVector[] dataset) {
        this.dataset = dataset;
    }

    public BaggedVector getRow(int rowId) {
        return dataset[rowId];
    }

    public int getRowsCount() {
        return dataset.length;
    }

    public void foreach(IgniteConsumer<BaggedVector> consumer) {
        for(BaggedVector vec : dataset)
            consumer.accept(vec);
    }

    @Override public void close() throws Exception {
        //NOP
    }
}
