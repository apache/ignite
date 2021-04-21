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

package org.apache.ignite.examples.ml.inference;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.inference.Model;
import org.apache.ignite.ml.inference.ModelDescriptor;
import org.apache.ignite.ml.inference.ModelSignature;
import org.apache.ignite.ml.inference.builder.SingleModelBuilder;
import org.apache.ignite.ml.inference.parser.IgniteModelParser;
import org.apache.ignite.ml.inference.reader.ModelStorageModelReader;
import org.apache.ignite.ml.inference.storage.descriptor.ModelDescriptorStorage;
import org.apache.ignite.ml.inference.storage.descriptor.ModelDescriptorStorageFactory;
import org.apache.ignite.ml.inference.storage.model.ModelStorage;
import org.apache.ignite.ml.inference.storage.model.ModelStorageFactory;

/**
 * This example demonstrates how to work with {@link ModelStorage}.
 */
public class ModelStorageExample {
    /**
     * Run example.
     */
    public static void main(String... args) throws IOException, ClassNotFoundException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite-ml.xml")) {
            System.out.println(">>> Ignite grid started.");

            ModelStorage storage = new ModelStorageFactory().getModelStorage(ignite);
            ModelDescriptorStorage descStorage = new ModelDescriptorStorageFactory().getModelDescriptorStorage(ignite);

            System.out.println("Saving model into model storage...");
            byte[] mdl = serialize((IgniteModel<byte[], byte[]>)i -> i);
            storage.mkdirs("/");
            storage.putFile("/my_model", mdl);

            System.out.println("Saving model descriptor into model descriptor storage...");
            ModelDescriptor desc = new ModelDescriptor(
                "MyModel",
                "My Cool Model",
                new ModelSignature("", "", ""),
                new ModelStorageModelReader("/my_model"),
                new IgniteModelParser<>()
            );
            descStorage.put("my_model", desc);

            System.out.println("List saved models...");
            for (IgniteBiTuple<String, ModelDescriptor> model : descStorage)
                System.out.println("-> {'" + model.getKey() + "' : " + model.getValue() + "}");

            System.out.println("Load saved model descriptor...");
            desc = descStorage.get("my_model");

            System.out.println("Build inference model...");
            SingleModelBuilder mdlBuilder = new SingleModelBuilder();
            try (Model<byte[], byte[]> infMdl = mdlBuilder.build(desc.getReader(), desc.getParser())) {

                System.out.println("Make inference...");
                for (int i = 0; i < 10; i++) {
                    Integer res = deserialize(infMdl.predict(serialize(i)));
                    System.out.println(i + " -> " + res);
                }
            }
        }
        finally {
            System.out.flush();
        }
    }

    /**
     * Serialized the specified object.
     *
     * @param o Object to be serialized.
     * @return Serialized object as byte array.
     * @throws IOException In case of exception.
     */
    private static <T extends Serializable> byte[] serialize(T o) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(o);
            oos.flush();

            return baos.toByteArray();
        }
    }

    /**
     * Deserialized object represented as a byte array.
     *
     * @param o   Serialized object.
     * @param <T> Type of serialized object.
     * @return Deserialized object.
     * @throws IOException            In case of exception.
     * @throws ClassNotFoundException In case of exception.
     */
    @SuppressWarnings("unchecked")
    private static <T extends Serializable> T deserialize(byte[] o) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(o);
             ObjectInputStream ois = new ObjectInputStream(bais)) {

            return (T)ois.readObject();
        }
    }
}
