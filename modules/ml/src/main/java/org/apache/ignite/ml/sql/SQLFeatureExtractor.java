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

package org.apache.ignite.ml.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;

/**
 * SQL feature extractor that should be used to extract features from binary objects stored in SQL table.
 */
public class SQLFeatureExtractor implements IgniteBiFunction<Object, BinaryObject, Vector> {
    /** Feature extractors for each needed fields as a list of functions. */
    private final List<Function<BinaryObject, Number>> featureExtractors = new ArrayList<>();

    /** {@inheritDoc} */
    @Override public Vector apply(Object o, BinaryObject obj) {
        Vector res = new DenseVector(featureExtractors.size());

        int i = 0;
        for (Function<BinaryObject, Number> featureExtractor : featureExtractors) {
            Number val = featureExtractor.apply(obj);

            if (val != null)
                res.set(i, val.doubleValue());

            i++;
        }

        return res;
    }

    /**
     * Adds feature extractor for the field with specified name and value transformer.
     *
     * @param name Field name.
     * @param transformer Field value transformer.
     * @param <T> Field type.
     * @return This SQL feature extractor.
     */
    public <T> SQLFeatureExtractor withField(String name, Function<T, Number> transformer) {
        featureExtractors.add(obj -> transformer.apply(obj.<T>field(name)));

        return this;
    }

    /**
     * Adds feature extractor for the field with specified name. Field should be numeric (subclass of {@link Number}).
     *
     * @param name Field name.
     * @return This SQL feature extractor.
     */
    public SQLFeatureExtractor withField(String name) {
        featureExtractors.add(obj -> obj.field(name));

        return this;
    }

    /**
     * Adds feature extractor for the field with specified name. Field should be numeric (subclass of {@link Number}).
     *
     * @param names Field names.
     * @return This SQL feature extractor.
     */
    public SQLFeatureExtractor withFields(String... names) {
        for (String name : names)
            withField(name);

        return this;
    }
}
