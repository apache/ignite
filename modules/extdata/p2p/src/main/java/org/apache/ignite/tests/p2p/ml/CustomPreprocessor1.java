/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.tests.p2p.ml;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 *
 */
public class CustomPreprocessor1 implements Preprocessor<Integer, Vector> {
    /** */
    private final Preprocessor<Integer, Vector> basePreprocessor;

    /** */
    public CustomPreprocessor1(Preprocessor<Integer, Vector> basePreprocessor) {
        this.basePreprocessor = basePreprocessor;
    }

    /** */
    @Override public LabeledVector apply(Integer integer, Vector vector) {
        LabeledVector res = basePreprocessor.apply(integer, vector);
        return res.features().normalize().labeled(res.label());
    }
}
