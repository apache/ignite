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

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.inference.builder.SingleModelBuilder;
import org.apache.ignite.ml.inference.parser.IgniteModelParser;
import org.apache.ignite.ml.inference.reader.ModelStorageModelReader;
import org.apache.ignite.ml.inference.storage.descriptor.ModelDescriptorStorage;
import org.apache.ignite.ml.inference.storage.descriptor.ModelDescriptorStorageFactory;
import org.apache.ignite.ml.inference.storage.model.ModelStorage;
import org.apache.ignite.ml.inference.storage.model.ModelStorageFactory;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.util.Utils;

public class IgniteModelStorageUtil {

    private static final String IGNITE_MDL_FOLDER = "/ignite_models";

    public static void save(IgniteModel<Vector, Double> mdl, String name) {
        Ignite ignite = Ignition.ignite();

        IgniteModel<byte[], byte[]> mdlWrapper = wrapIgniteModel(mdl);
        byte[] serializedMdl = Utils.serialize(mdlWrapper);
        UUID mdlId = UUID.randomUUID();

        // Save serialized model into model storage.
        ModelStorage storage = new ModelStorageFactory().getModelStorage(ignite);
        storage.mkdirs(IGNITE_MDL_FOLDER);
        storage.putFile(IGNITE_MDL_FOLDER + "/" + mdlId, serializedMdl);

        ModelDescriptorStorage descStorage = new ModelDescriptorStorageFactory().getModelDescriptorStorage(ignite);
        descStorage.put(name, new ModelDescriptor(
            name,
            null,
            new ModelSignature(null, null, null),
            new ModelStorageModelReader(IGNITE_MDL_FOLDER + "/" + mdlId),
            new IgniteModelParser<>()
        ));
    }

    public static Model<Vector, Double> get(String name) {
        Ignite ignite = Ignition.ignite();

        ModelDescriptorStorage descStorage = new ModelDescriptorStorageFactory().getModelDescriptorStorage(ignite);
        ModelDescriptor desc = descStorage.get(name);

        Model<byte[], byte[]> infMdl = new SingleModelBuilder().build(desc.getReader(), desc.getParser());

        return unwrapIgniteModel(infMdl);
    }

    private static IgniteModel<byte[], byte[]> wrapIgniteModel(IgniteModel<Vector, Double> mdl) {
        return input -> {
            Vector deserializedInput = Utils.deserialize(input);
            Double output = mdl.predict(deserializedInput);

            return Utils.serialize(output);
        };
    }

    private static Model<Vector, Double> unwrapIgniteModel(Model<byte[], byte[]> mdl) {
        return new Model<Vector, Double>() {
            @Override public Double predict(Vector input) {
                byte[] serializedInput = Utils.serialize(input);
                byte[] serializedOutput = mdl.predict(serializedInput);

                return Utils.deserialize(serializedOutput);
            }

            @Override public void close() {
                mdl.close();
            }
        };
    }
}
