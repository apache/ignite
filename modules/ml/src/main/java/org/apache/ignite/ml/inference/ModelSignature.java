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

package org.apache.ignite.ml.inference;

import java.io.Serializable;

/**
 * Signature that defines input/output types in Protobuf.
 */
public class ModelSignature implements Serializable {
    /** Protobuf schema of all objects required in the model. */
    private final String schema;

    /** Name of the input type (should be presented in the {@link #schema}. */
    private final String inputMsg;

    /** Name of ths output type (should be presented in the {@link #schema}). */
    private final String outputMsg;

    /**
     * Constructs a new instance of model signature.
     *
     * @param schema Protobuf schema of all objects required in the model.
     * @param inputMsg Name of the input type (should be presented in the {@link #schema}.
     * @param outputMsg Name of ths output type (should be presented in the {@link #schema}).
     */
    public ModelSignature(String schema, String inputMsg, String outputMsg) {
        this.schema = schema;
        this.inputMsg = inputMsg;
        this.outputMsg = outputMsg;
    }

    /** */
    public String getSchema() {
        return schema;
    }

    /** */
    public String getInputMsg() {
        return inputMsg;
    }

    /** */
    public String getOutputMsg() {
        return outputMsg;
    }
}
