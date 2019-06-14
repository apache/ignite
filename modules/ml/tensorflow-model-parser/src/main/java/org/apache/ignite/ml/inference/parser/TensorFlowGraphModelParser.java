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

import org.tensorflow.Graph;
import org.tensorflow.Session;

/**
 * Implementation of TensorFlow model parser that accepts serialized graph definition.
 *
 * @param <I> Type of model input.
 * @param <O> Type of model output.
 */
public class TensorFlowGraphModelParser<I, O> extends TensorFlowBaseModelParser<I, O> {
    /** */
    private static final long serialVersionUID = -1872566748640565856L;

    /** {@inheritDoc} */
    @Override public Session parseModel(byte[] mdl) {
        Graph graph = new Graph();
        graph.importGraphDef(mdl);

        return new Session(graph);
    }
}
