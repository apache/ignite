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

package org.apache.ignite.ml.optimization.util;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.distributed.keys.RowColMatrixKey;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.ml.math.impls.storage.matrix.SparseDistributedMatrixStorage;

/**
 * Wrapper of {@link SparseDistributedMatrix} which allow to perform computation on every node containing a part of the
 * distributed matrix, get results and then reduce them.
 */
public class SparseDistributedMatrixMapReducer {
    /** */
    private final SparseDistributedMatrix distributedMatrix;

    /** */
    public SparseDistributedMatrixMapReducer(
        SparseDistributedMatrix distributedMatrix) {
        this.distributedMatrix = distributedMatrix;
    }

    /** */
    public <R, T> R mapReduce(IgniteBiFunction<Matrix, T, R> mapper, IgniteFunction<Collection<R>, R> reducer, T args) {
        Ignite ignite = Ignition.localIgnite();
        SparseDistributedMatrixStorage storage = (SparseDistributedMatrixStorage)distributedMatrix.getStorage();
        int columnSize = distributedMatrix.columnSize();
        Collection<R> results = ignite
            .compute(ignite.cluster().forDataNodes(storage.cacheName()))
            .broadcast(arguments -> {
                Ignite ig = Ignition.localIgnite();
                Affinity<RowColMatrixKey> affinity = ig.affinity(storage.cacheName());
                ClusterNode localNode = ig.cluster().localNode();
                Map<ClusterNode, Collection<RowColMatrixKey>> keys = affinity.mapKeysToNodes(storage.getAllKeys());
                Collection<RowColMatrixKey> locKeys = keys.get(localNode);
                if (locKeys != null) {
                    Matrix localMatrix = new DenseLocalOnHeapMatrix(locKeys.size(), columnSize);
                    int index = 0;
                    for (RowColMatrixKey key : locKeys) {
                        Map<Integer, Double> row = storage.cache().get(key);
                        for (Map.Entry<Integer,Double> cell : row.entrySet()) {
                            localMatrix.set(index, cell.getKey(), cell.getValue());
                        }
                        index++;
                    }
                    return mapper.apply(localMatrix, arguments);
                }
                return null;
            }, args);
        return reducer.apply(results);
    }
}
