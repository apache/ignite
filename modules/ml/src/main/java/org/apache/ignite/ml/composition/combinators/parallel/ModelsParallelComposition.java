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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.ml.IgniteModel;

/**
 * Parallel composition of models.
 * Parallel composition of models is a model which contains a list of models with same input and output types.
 *
 * @param <I>
 * @param <O>
 */
public class ModelsParallelComposition<I, O> implements IgniteModel<I, List<O>> {
    private final List<IgniteModel<I, O>> models;

    public ModelsParallelComposition(List<IgniteModel<I, O>> models) {
        this.models = models;
    }

    @Override public List<O> predict(I i) {
        return models
            .stream()
            .map(m -> m.predict(i))
            .collect(Collectors.toList());
    }

    public List<IgniteModel<I, O>> models() {
        return new ArrayList<>(models);
    }
}
