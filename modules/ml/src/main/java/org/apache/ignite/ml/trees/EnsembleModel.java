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

package org.apache.ignite.ml.trees;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteFunction;

public class EnsembleModel<D, T> implements Model<D, Stream<T>> {
    protected List<Model<D, T>> ensemble;

    protected EnsembleModel(List<Model<D, T>> mdls) {
        ensemble = new ArrayList<>(mdls);
    }

    public static <D, T> EnsembleModel<D, T> of(Model<D, T> ... mdls) {
        return new EnsembleModel<>(Arrays.asList(mdls));
    }

    public static <D, T> EnsembleModel<D, T> of(List<Model<D, T>> mdls) {
        return new EnsembleModel<>(mdls);
    }

    public <V> EnsembleModel<D, V> map(IgniteFunction<T, V> f) {
        List<Model<D, V>> newEns = ensemble.stream().map(model -> (Model<D, V>)val -> f.apply(model.predict(val))).collect(Collectors.toList());
        return EnsembleModel.of(newEns);
    }

    @Override public Stream<T> predict(D val) {
        return ensemble.stream().map(mdl -> mdl.predict(val));
    }
}
