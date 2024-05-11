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

package org.apache.ignite.ml.composition.combinators.parallel;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.ml.IgniteModel;

/**
 * Parallel composition of models.
 * Parallel composition of models is a model which contains a list of submodels with same input and output types.
 * Result of prediction in such model is a list of predictions of each of submodels.
 *
 * @param <I> Type of submodel input.
 * @param <O> Type of submodel output.
 */
public final class ModelsParallelComposition<I, O> implements IgniteModel<I, List<O>> {
    /** List of submodels. */
    private final List<IgniteModel<I, O>> submodels;

    /**
     * Construc an instance of this class from list of submodels.
     *
     * @param submodels List of submodels constituting this model.
     */
    public ModelsParallelComposition(List<IgniteModel<I, O>> submodels) {
        this.submodels = submodels;
    }

    /** {@inheritDoc} */
    @Override public List<O> predict(I i) {
        return submodels
            .stream()
            .map(m -> m.predict(i))
            .collect(Collectors.toList());
    }

    /**
     * List of submodels constituting this model.
     *
     * @return List of submodels constituting this model.
     */
    public List<IgniteModel<I, O>> submodels() {
        return Collections.unmodifiableList(submodels);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        submodels.forEach(IgniteModel::close);
    }
}
