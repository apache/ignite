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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import org.apache.ignite.ml.inference.InfModel;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Implementation of model parser that accepts serialized {@link IgniteFunction}.
 *
 * @param <I> Type of model input.
 * @param <O> Type of model output.
 */
public class IgniteFunctionInfModelParser<I, O> implements InfModelParser<I, O> {
    /** */
    private static final long serialVersionUID = -4624683614990816434L;

    /** {@inheritDoc} */
    @Override public InfModel<I, O> parse(byte[] mdl) {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(mdl);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            @SuppressWarnings("unchecked")
            IgniteFunction<I, O> function = (IgniteFunction<I, O>)ois.readObject();

            return new IgniteFunctionInfoModel(function);
        }
        catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Inference model that wraps {@link IgniteFunction}.
     */
    private class IgniteFunctionInfoModel implements InfModel<I, O> {
        /** Ignite function. */
        private final IgniteFunction<I, O> function;

        /**
         * Constructs a new instance of Ignite function.
         *
         * @param function Ignite function.
         */
        IgniteFunctionInfoModel(IgniteFunction<I, O> function) {
            this.function = function;
        }

        /** {@inheritDoc} */
        @Override public O predict(I input) {
            return function.apply(input);
        }

        /** {@inheritDoc} */
        @Override public void close() {
            // Do nothing.
        }
    }
}
