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

package org.apache.ignite.ml.inference.storage.descriptor;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.inference.ModelDescriptor;

/**
 * Model descriptor storage factory. Provides {@link ModelDescriptorStorage}.
 */
public class ModelDescriptorStorageFactory {
    /** Model descriptor storage cache name. */
    public static final String MODEL_DESCRIPTOR_STORAGE_CACHE_NAME = "MODEL_DESCRIPTOR_STORAGE";

    /**
     * Returns model descriptor storage based on Apache Ignite cache.
     *
     * @param ignite Ignite instance.
     * @return Model descriptor storage.
     */
    public ModelDescriptorStorage getModelDescriptorStorage(Ignite ignite) {
        IgniteCache<String, ModelDescriptor> cache = ignite.cache(MODEL_DESCRIPTOR_STORAGE_CACHE_NAME);

        if (cache == null)
            throw new IllegalStateException("Model descriptor storage doesn't exists. Enable ML plugin to create it.");

        return new IgniteModelDescriptorStorage(cache);
    }

}
