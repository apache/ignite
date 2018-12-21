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

package org.apache.ignite.ml.pipeline;

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Wraps the model produced by {@link Pipeline}.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class PipelineMdl<K, V> implements Model<Vector, Double> {
    /** Internal model produced by {@link Pipeline}. */
    private Model<Vector, Double> internalMdl;

    /** Feature extractor. */
    private IgniteBiFunction<K, V, Vector> featureExtractor;

    /** Label extractor. */
    private IgniteBiFunction<K, V, Double> lbExtractor;

    /** */
    @Override public Double apply(Vector vector) {
        return internalMdl.apply(vector);
    }

    /** */
    public IgniteBiFunction<K, V, Vector> getFeatureExtractor() {
        return featureExtractor;
    }

    /** */
    public IgniteBiFunction<K, V, Double> getLabelExtractor() {
        return lbExtractor;
    }

    /** */
    public Model<Vector, Double> getInternalMdl() {
        return internalMdl;
    }

    /** */
    public PipelineMdl<K, V> withInternalMdl(Model<Vector, Double> internalMdl) {
        this.internalMdl = internalMdl;
        return this;
    }

    /** */
    public PipelineMdl<K, V> withFeatureExtractor(IgniteBiFunction featureExtractor) {
        this.featureExtractor = featureExtractor;
        return this;
    }

    /** */
    public PipelineMdl<K, V> withLabelExtractor(IgniteBiFunction<K, V, Double> lbExtractor) {
        this.lbExtractor = lbExtractor;
        return this;
    }

    /** */
    @Override public String toString() {
        return "PipelineMdl{" +
            "internalMdl=" + internalMdl +
            '}';
    }
}
