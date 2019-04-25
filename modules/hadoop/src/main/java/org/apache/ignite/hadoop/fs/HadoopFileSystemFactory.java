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

import org.apache.ignite.lifecycle.LifecycleAware;

import java.io.IOException;
import java.io.Serializable;

/**
 * Factory for Hadoop {@code FileSystem} used by {@link IgniteHadoopIgfsSecondaryFileSystem}.
 * <p>
 * {@link #get(String)} method will be used whenever a call to a target {@code FileSystem} is required.
 * <p>
 * It is implementation dependent whether to rely on built-in Hadoop file system cache, implement own caching facility
 * or doesn't cache file systems at all.
 * <p>
 * Concrete factory may implement {@link LifecycleAware} interface. In this case start and stop callbacks will be
 * performed by Ignite. You may want to implement some initialization or cleanup there.
 */
public interface HadoopFileSystemFactory extends Serializable {
    /**
     * Gets file system for the given user name.
     *
     * @param usrName User name
     * @return File system.
     * @throws IOException In case of error.
     */
    public Object get(String usrName) throws IOException;
}
