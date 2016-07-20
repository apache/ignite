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

package org.apache.ignite.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.hadoop.fs.HadoopFileSystemsUtils;
import org.apache.ignite.internal.processors.hadoop.fs.HadoopLazyConcurrentMap;

import java.io.IOException;
import java.net.URI;

/**
 * Caching Hadoop file system factory. Caches {@link FileSystem} instances on per-user basis. Doesn't rely on
 * built-in Hadoop {@code FileSystem} caching mechanics. Separate {@code FileSystem} instance is created for each
 * user instead.
 * <p>
 * This makes cache instance resistant to concurrent calls to {@link FileSystem#close()} in other parts of the user
 * code. On the other hand, this might cause problems on some environments. E.g. if Kerberos is enabled, a call to
 * {@link FileSystem#get(URI, Configuration, String)} will refresh Kerberos token. But this factory implementation
 * calls this method only once per user what may lead to token expiration. In such cases it makes sense to either
 * use {@link BasicHadoopFileSystemFactory} or implement your own factory.
 */
public class CachingHadoopFileSystemFactory extends BasicHadoopFileSystemFactory {
    /** */
    private static final long serialVersionUID = 0L;

    /** Per-user file system cache. */
    private final transient HadoopLazyConcurrentMap<String, FileSystem> cache = new HadoopLazyConcurrentMap<>(
        new HadoopLazyConcurrentMap.ValueFactory<String, FileSystem>() {
            @Override public FileSystem createValue(String key) throws IOException {
                return CachingHadoopFileSystemFactory.super.getWithMappedName(key);
            }
        }
    );

    /**
     * Public non-arg constructor.
     */
    public CachingHadoopFileSystemFactory() {
        // noop
    }

    /** {@inheritDoc} */
    @Override public FileSystem getWithMappedName(String name) throws IOException {
        return cache.getOrCreate(name);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteException {
        super.start();

        // Disable caching.
        cfg.setBoolean(HadoopFileSystemsUtils.disableFsCachePropertyName(fullUri.getScheme()), true);
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteException {
        super.stop();

        try {
            cache.close();
        }
        catch (IgniteCheckedException ice) {
            throw new IgniteException(ice);
        }
    }
}
