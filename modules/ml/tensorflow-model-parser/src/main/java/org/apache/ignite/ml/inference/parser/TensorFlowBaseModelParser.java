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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.ml.inference.Model;
import org.tensorflow.Session;
import org.tensorflow.Tensor;

/**
 * Base class for TensorFlow model parsers. Contains the logic that is common for models saved as "SavedModel" and as a
 * simple graph.
 *
 * @param <I> Type of model input.
 * @param <O> Type of model output.
 */
public abstract class TensorFlowBaseModelParser<I, O> implements ModelParser<I, O, Model<I, O>> {
    /** */
    private static final long serialVersionUID = 5574259553625871456L;

    /** Map of input graph nodes (placeholders) and transformers that allow to transform input into tensor. */
    private final Map<String, InputTransformer<I>> inputs = new HashMap<>();

    /** List of output graph nodes. */
    private List<String> outputNames;

    /** Transformer that allows to transform tensors into output. */
    private OutputTransformer<O> outputTransformer;

    /** {@inheritDoc} */
    @Override public Model<I, O> parse(byte[] mdl) {
        return new TensorFlowInfModel(parseModel(mdl));
    }

    /**
     * Parses model specified in serialized form as byte array.
     *
     * @param mdl Inference model in serialized form as byte array.
     * @return TensorFlow session that encapsulates the TensorFlow graph parsed from serialized model.
     */
    public abstract Session parseModel(byte[] mdl);

    /**
     * Setter that allows to specify additional input graph node and correspondent transformer that allows to transform
     * input into tensor.
     *
     * @param name Name of the input graph node.
     * @param transformer Transformer that allows to transform input into tensor.
     * @return This instance.
     */
    public TensorFlowBaseModelParser<I, O> withInput(String name, InputTransformer<I> transformer) {
        if (inputs.containsKey(name))
            throw new IllegalArgumentException("Inputs already contains specified name [name=" + name + "]");

        inputs.put(name, transformer);

        return this;
    }

    /**
     * Setter that allows to specify output graph nodes and correspondent transformer that allow to transform tensors
     * into output.
     *
     * @param names List of output graph node names.
     * @param transformer Transformer that allow to transform tensors into output.
     * @return This instance.
     */
    public TensorFlowBaseModelParser<I, O> withOutput(List<String> names, OutputTransformer<O> transformer) {
        if (outputNames != null || outputTransformer != null)
            throw new IllegalArgumentException("Outputs already specified");

        outputNames = names;
        outputTransformer = transformer;

        return this;
    }

    /**
     * Input transformer that accepts input and transforms it into tensor.
     *
     * @param <I> Type of model input.
     */
    @FunctionalInterface
    public interface InputTransformer<I> extends Serializable {
        /**
         * Transforms input into tensor.
         *
         * @param input Input data.
         * @return Tensor (transformed input data).
         */
        public Tensor<?> transform(I input);
    }

    /**
     * Output transformer that accepts tensors and transforms them into output.
     *
     * @param <O> Type of model output.
     */
    @FunctionalInterface
    public interface OutputTransformer<O> extends Serializable {
        /**
         * Transforms tensors into output.
         *
         * @param output Tensors.
         * @return Output (transformed tensors).
         */
        public O transform(Map<String, Tensor<?>> output);
    }

    /**
     * TensorFlow inference model based on pre-loaded graph and created session.
     */
    private class TensorFlowInfModel implements Model<I, O> {
        /** TensorFlow session. */
        private final Session ses;

        /**
         * Constructs a new instance of TensorFlow inference model.
         *
         * @param ses TensorFlow session.
         */
        TensorFlowInfModel(Session ses) {
            this.ses = ses;
        }

        /** {@inheritDoc} */
        @Override public O predict(I input) {
            Session.Runner runner = ses.runner();

            runner = feedAll(runner, input);
            runner = fetchAll(runner);

            List<Tensor<?>> prediction = runner.run();
            Map<String, Tensor<?>> collectedPredictionTensors = indexTensors(prediction);

            return outputTransformer.transform(collectedPredictionTensors);
        }

        /**
         * Feeds input into graphs input nodes using input transformers (see {@link #inputs}).
         *
         * @param runner TensorFlow session runner.
         * @param input Input.
         * @return TensorFlow session runner.
         */
        private Session.Runner feedAll(Session.Runner runner, I input) {
            for (Map.Entry<String, InputTransformer<I>> e : inputs.entrySet()) {
                String opName = e.getKey();
                InputTransformer<I> transformer = e.getValue();

                runner = runner.feed(opName, transformer.transform(input));
            }

            return runner;
        }

        /**
         * Specifies graph output nodes to be fetched using {@link #outputNames}.
         *
         * @param runner TensorFlow session runner.
         * @return TensorFlow session runner.
         */
        private Session.Runner fetchAll(Session.Runner runner) {
            for (String e : outputNames)
                runner.fetch(e);

            return runner;
        }

        /**
         * Indexes tensors fetched from graph using {@link #outputNames}.
         *
         * @param tensors List of fetched tensors.
         * @return Map of tensor name as a key and tensor as a value.
         */
        private Map<String, Tensor<?>> indexTensors(List<Tensor<?>> tensors) {
            Map<String, Tensor<?>> collectedTensors = new HashMap<>();

            Iterator<String> outputNamesIter = outputNames.iterator();
            Iterator<Tensor<?>> tensorsIter = tensors.iterator();

            while (outputNamesIter.hasNext() && tensorsIter.hasNext())
                collectedTensors.put(outputNamesIter.next(), tensorsIter.next());

            // We expect that output names and output tensors have the same size.
            if (outputNamesIter.hasNext() || tensorsIter.hasNext())
                throw new IllegalStateException("Outputs are incorrect");

            return collectedTensors;
        }

        /** {@inheritDoc} */
        @Override public void close() {
            ses.close();
        }
    }
}
