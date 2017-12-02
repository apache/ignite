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

package org.apache.ignite.ml.trees.trainers.columnbased;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.distributed.keys.RowColMatrixKey;
import org.apache.ignite.ml.math.distributed.keys.impl.SparseMatrixKey;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.ml.math.impls.storage.matrix.SparseDistributedMatrixStorage;
import org.apache.ignite.ml.math.StorageConstants;
import org.jetbrains.annotations.NotNull;

/**
 * Adapter of SparseDistributedMatrix to ColumnDecisionTreeTrainerInput.
 * Sparse SparseDistributedMatrix should be in {@link StorageConstants#COLUMN_STORAGE_MODE} and
 * should contain samples in rows last position in row being label of this sample.
 */
public class MatrixColumnDecisionTreeTrainerInput extends CacheColumnDecisionTreeTrainerInput<RowColMatrixKey, Map<Integer, Double>> {
    /**
     * @param m Sparse SparseDistributedMatrix should be in {@link StorageConstants#COLUMN_STORAGE_MODE}
     * containing samples in rows last position in row being label of this sample.
     * @param catFeaturesInfo Information about which features are categorical in form of feature index -> number of
     * categories.
     */
    public MatrixColumnDecisionTreeTrainerInput(SparseDistributedMatrix m, Map<Integer, Integer> catFeaturesInfo) {
        super(((SparseDistributedMatrixStorage)m.getStorage()).cache(),
            () -> Stream.of(new SparseMatrixKey(m.columnSize() - 1, m.getUUID(), m.columnSize() - 1)),
            valuesMapper(m),
            labels(m),
            keyMapper(m),
            catFeaturesInfo,
            m.columnSize() - 1,
            m.rowSize());
    }

    /** Values mapper. See {@link CacheColumnDecisionTreeTrainerInput#valuesMapper} */
    @NotNull
    private static IgniteFunction<Cache.Entry<RowColMatrixKey, Map<Integer, Double>>, Stream<IgniteBiTuple<Integer, Double>>> valuesMapper(
        SparseDistributedMatrix m) {
        return ent -> {
            Map<Integer, Double> map = ent.getValue() != null ? ent.getValue() : new HashMap<>();
            return IntStream.range(0, m.rowSize()).mapToObj(k -> new IgniteBiTuple<>(k, map.getOrDefault(k, 0.0)));
        };
    }

    /** Key mapper. See {@link CacheColumnDecisionTreeTrainerInput#keyMapper} */
    @NotNull private static IgniteFunction<Integer, Stream<RowColMatrixKey>> keyMapper(SparseDistributedMatrix m) {
        return i -> Stream.of(new SparseMatrixKey(i, ((SparseDistributedMatrixStorage)m.getStorage()).getUUID(), i));
    }

    /** Labels mapper. See {@link CacheColumnDecisionTreeTrainerInput#labelsMapper} */
    @NotNull private static IgniteFunction<Map<Integer, Double>, DoubleStream> labels(SparseDistributedMatrix m) {
        return mp -> IntStream.range(0, m.rowSize()).mapToDouble(k -> mp.getOrDefault(k, 0.0));
    }

    /** {@inheritDoc} */
    @Override public Object affinityKey(int idx, Ignite ignite) {
        return idx;
    }
}
