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

package org.apache.ignite.ml.inference.parser;

import java.io.Serializable;
import org.apache.ignite.ml.inference.Model;

/**
 * Model parser that accepts a serialized model represented by byte array, parses it and returns {@link Model}.
 *
 * @param <I> Type of model input.
 * @param <O> Type of model output.
 */
@FunctionalInterface
public interface ModelParser<I, O, M extends Model<I, O>> extends Serializable {
    /**
     * Accepts serialized model represented by byte array, parses it and returns {@link Model}.
     *
     * @param mdl Serialized model represented by byte array.
     * @return Inference model.
     */
    public M parse(byte[] mdl);
}
