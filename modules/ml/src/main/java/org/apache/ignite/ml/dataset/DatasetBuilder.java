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

package org.apache.ignite.ml.dataset;

import java.io.Serializable;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;

/**
 * A builder constructing instances of a {@link Dataset}. Implementations of this interface encapsulate logic of
 * building specific datasets such as allocation required data structures and initialization of {@code context} part of
 * partitions.
 *
 * @param <C> type of a partition {@code context}
 * @param <D> type of a partition {@code data}
 *
 * @see CacheBasedDatasetBuilder
 * @see LocalDatasetBuilder
 * @see Dataset
 */
public interface DatasetBuilder<C extends Serializable, D extends AutoCloseable> {
    /**
     * Constructs a new instance of {@link Dataset} that includes allocation required data structures and
     * initialization of {@code context} part of partitions.
     *
     * @return dataset
     */
    public Dataset<C, D> build();
}
