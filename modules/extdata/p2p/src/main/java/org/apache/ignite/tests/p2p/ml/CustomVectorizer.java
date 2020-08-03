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

package org.apache.ignite.tests.p2p.ml;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 *
 */
public class CustomVectorizer extends Vectorizer<Integer, Vector, Integer, Double> {
    /** {@inheritDoc} */
    @Override protected Serializable feature(Integer coord, Integer key, Vector value) {
        return value.get(coord);
    }

    /** {@inheritDoc} */
    @Override protected Double label(Integer coord, Integer key, Vector value) {
        return value.get(coord);
    }

    /** {@inheritDoc} */
    @Override protected Double zero() {
        return 0.0;
    }

    /** {@inheritDoc} */
    @Override protected List<Integer> allCoords(Integer key, Vector value) {
        return IntStream.range(0, value.size()).boxed().collect(Collectors.toList());
    }
}
