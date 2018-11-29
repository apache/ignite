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

package org.apache.ignite.ml.inference.storage;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.inference.ModelDescriptor;

/**
 * Model descriptor storage based on Apache Ignite cache.
 */
public class IgniteModelDescriptorStorage implements ModelDescriptorStorage {
    /** Apache Ignite cache name to keep model descriptors. */
    private static final String MODEL_DESCRIPTOR_CACHE_NAME = "MODEL_DESCRIPTOR_CACHE";

    /** Apache Ignite cache to keep model descriptors. */
    private final IgniteCache<String, ModelDescriptor> models;

    /**
     * Constructs a new instance of Ignite model descriptor storage.
     *
     * @param ignite Ignite instance.
     */
    public IgniteModelDescriptorStorage(Ignite ignite) {
        models = ignite.getOrCreateCache(MODEL_DESCRIPTOR_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override public void put(String mdlId, ModelDescriptor mdl) {
        models.put(mdlId, mdl);
    }

    /** {@inheritDoc} */
    @Override public ModelDescriptor get(String mdlId) {
        return models.get(mdlId);
    }

    /** {@inheritDoc} */
    @Override public void remove(String mdlId) {
        models.remove(mdlId);
    }
}
