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

package org.apache.ignite.ml.dataset.feature.extractor.impl;

import java.io.Serializable;
import org.apache.ignite.ml.dataset.feature.extractor.ExtractionUtils;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Vectorizer on Vector.
 *
 * @param <K> Type of key.
 */
public class DummyVectorizer<K> extends ExtractionUtils.ArrayLikeVectorizer<K, Vector> {
    /** Serial version uid. */
    private static final long serialVersionUID = -6225354615212148224L;

    /**
     * Creates an instance of Vectorizer.
     *
     * @param coords Coordinates.
     */
    public DummyVectorizer(Integer ... coords) {
        super(coords);
    }

    /** {@inheritDoc} */
    @Override protected Serializable feature(Integer coord, K key, Vector value) {
        return value.getRaw(coord);
    }

    /** {@inheritDoc} */
    @Override protected int sizeOf(K key, Vector value) {
        return value.size();
    }

}
