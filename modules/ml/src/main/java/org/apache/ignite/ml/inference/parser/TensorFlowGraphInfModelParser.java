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

package org.apache.ignite.ml.inference.parser;

import org.tensorflow.Graph;
import org.tensorflow.Session;

/**
 * Implementation of TensorFlow model parser that accepts serialized graph definition.
 *
 * @param <I> Type of model input.
 * @param <O> Type of model output.
 */
public class TensorFlowGraphInfModelParser<I, O> extends TensorFlowBaseInfModelParser<I, O> {
    /** */
    private static final long serialVersionUID = -1872566748640565856L;

    /** {@inheritDoc} */
    @Override public Session parseModel(byte[] mdl) {
        Graph graph = new Graph();
        graph.importGraphDef(mdl);

        return new Session(graph);
    }
}
