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

import org.apache.ignite.ml.IgniteModel;

public class ModelsSequentialComposition<I, O1, O2> implements IgniteModel<I, O2> {
    private IgniteModel<I, O1> mdl1;
    private IgniteModel<O1, O2> mdl2;

    public IgniteModel<I, O1> firstModel() {
        return mdl1;
    }

    public IgniteModel<O1, O2> secondModel() {
        return mdl2;
    }

    public ModelsSequentialComposition(IgniteModel<I, O1> mdl1, IgniteModel<O1, O2> mdl2) {
        this.mdl1 = mdl1;
        this.mdl2 = mdl2;
    }

    @Override public O2 predict(I i1) {
        return mdl1.andThen(mdl2).predict(i1);
    }
}
