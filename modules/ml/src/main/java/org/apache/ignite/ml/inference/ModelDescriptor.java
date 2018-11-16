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
import org.apache.ignite.ml.inference.parser.InfModelParser;
import org.apache.ignite.ml.inference.reader.InfModelReader;

/**
 * Model descriptor that encapsulates information about model, {@link InfModelReader} and {@link InfModelParser} which
 * is required to build the model.
 */
public class ModelDescriptor implements Serializable {
    /** Model name. */
    private final String name;

    /** Model description. */
    private final String desc;

    /** Model signature that keeps input/output types in Protobuf. */
    private final ModelSignature signature;

    /** Model reader. */
    private final InfModelReader reader;

    /** Model parser. */
    private final InfModelParser<byte[], byte[]> parser;

    /**
     * Constructs a new instance of model descriptor.
     *
     * @param name Model name.
     * @param desc Model description.
     * @param signature Model signature that keeps input/output types in Protobuf.
     * @param reader Model reader.
     * @param parser Model parser.
     */
    public ModelDescriptor(String name, String desc, ModelSignature signature, InfModelReader reader,
        InfModelParser<byte[], byte[]> parser) {
        this.name = name;
        this.desc = desc;
        this.signature = signature;
        this.reader = reader;
        this.parser = parser;
    }

    /** */
    public String getName() {
        return name;
    }

    /** */
    public String getDesc() {
        return desc;
    }

    /** */
    public ModelSignature getSignature() {
        return signature;
    }

    /** */
    public InfModelReader getReader() {
        return reader;
    }

    /** */
    public InfModelParser<byte[], byte[]> getParser() {
        return parser;
    }
}
