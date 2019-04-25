/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.inference.storage.model;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

/**
 * Model storage factory. Provides {@link ModelStorage}.
 */
public class ModelStorageFactory {
    /** Model storage cache name. */
    public static final String MODEL_STORAGE_CACHE_NAME = "MODEL_STORAGE";

    /**
     * Returns model storage based on Apache Ignite cache.
     *
     * @param ignite Ignite instance.
     * @return Model storage.
     */
    public ModelStorage getModelStorage(Ignite ignite) {
        IgniteCache<String, FileOrDirectory> cache = ignite.cache(MODEL_STORAGE_CACHE_NAME);

        if (cache == null)
            throw new IllegalStateException("Model storage doesn't exists. Enable ML plugin to create it.");

        ModelStorageProvider storageProvider = new IgniteModelStorageProvider(cache);

        return new DefaultModelStorage(storageProvider);
    }
}
