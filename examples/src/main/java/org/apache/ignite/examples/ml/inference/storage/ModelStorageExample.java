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

package org.apache.ignite.examples.ml.inference.storage;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.ml.inference.plugin.MLInferencePluginConfiguration;
import org.apache.ignite.ml.inference.storage.model.ModelStorage;
import org.apache.ignite.ml.inference.storage.model.ModelStorageFactory;

/**
 * This example demonstrates how to work with {@link ModelStorage}.
 */
public class ModelStorageExample {
    /** Run example. */
    public static void main(String... args) {
        IgniteConfiguration cfg = new IgniteConfiguration();
        MLInferencePluginConfiguration pc = new MLInferencePluginConfiguration();
        pc.setWithModelDescriptorStorage(true);
        pc.setWithModelStorage(true);
        cfg.setPluginConfigurations(pc);

        try (Ignite ignite = Ignition.start(cfg)) {
            System.out.println(">>> Ignite grid started.");

            ModelStorage storage = ModelStorageFactory.getModelStorage(ignite);
            storage.mkdirs("/");
            storage.putFile("/a", new byte[100000]);

            byte[] data = storage.getFile("/a");
            System.out.println("Data size: " + data.length);
        }
    }
}
