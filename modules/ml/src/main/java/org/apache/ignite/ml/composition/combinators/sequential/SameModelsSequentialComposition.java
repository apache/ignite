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

package org.apache.ignite.ml.composition.combinators.sequential;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.functions.IgniteFunction;

public class SameModelsSequentialComposition<I, O>
    implements IgniteModel<I, O> {
    private final IgniteFunction<O, I> f;
    private final List<IgniteModel<I, O>> mdls;
    private final IgniteModel<I, O> finalMdl;

    public SameModelsSequentialComposition(IgniteFunction<O, I> f, List<? extends IgniteModel<I, O>> mdls) {
        this.f = f;
        this.mdls = new ArrayList<>(mdls);
        IgniteModel<I, O> fn = mdls.get(0);

        for (IgniteModel<I, O> m : mdls.subList(1, mdls.size()))
            fn = fn.andThen(f).andThen(m);

        finalMdl = fn;
    }

    /** {@inheritDoc} */
    @Override public O predict(I i) {
        return finalMdl.predict(i);
    }

    public SameModelsSequentialComposition<I, O> addModel(IgniteModel<I, O> mdl) {
        mdls.add(mdl);

        return this;
    }
}
