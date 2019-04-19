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

package org.apache.ignite.hadoop.fs;

/**
 * Caching Hadoop file system factory. Caches {@code FileSystem} instances on per-user basis. Doesn't rely on
 * built-in Hadoop {@code FileSystem} caching mechanics. Separate {@code FileSystem} instance is created for each
 * user instead.
 * <p>
 * This makes cache instance resistant to concurrent calls to {@code FileSystem.close()} in other parts of the user
 * code. On the other hand, this might cause problems on some environments. E.g. if Kerberos is enabled, a call to
 * {@code FileSystem.get(URI, Configuration, String)} will refresh Kerberos token. But this factory implementation
 * calls this method only once per user what may lead to token expiration. In such cases it makes sense to either
 * use {@link BasicHadoopFileSystemFactory} or implement your own factory.
 */
public class CachingHadoopFileSystemFactory extends BasicHadoopFileSystemFactory {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Constructor.
     */
    public CachingHadoopFileSystemFactory() {
        // No-op.
    }
}
