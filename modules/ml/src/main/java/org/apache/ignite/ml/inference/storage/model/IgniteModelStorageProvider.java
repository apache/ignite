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

import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCache;

/**
 * Implementation of {@link ModelStorageProvider} based on Apache Ignite cache.
 */
public class IgniteModelStorageProvider implements ModelStorageProvider {
    /** Storage of the files and directories. */
    private final IgniteCache<String, FileOrDirectory> cache;

    /**
     * Constructs a new instance of Ignite model storage provider.
     *
     * @param cache Storage of the files and directories.
     */
    public IgniteModelStorageProvider(IgniteCache<String, FileOrDirectory> cache) {
        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override public FileOrDirectory get(String path) {
        return cache.get(path);
    }

    /** {@inheritDoc} */
    @Override public void put(String path, FileOrDirectory file) {
        cache.put(path, file);
    }

    /** {@inheritDoc} */
    @Override public void remove(String path) {
        cache.remove(path);
    }

    /** {@inheritDoc} */
    @Override public Lock lock(String path) {
        return cache.lock(path);
    }
}
