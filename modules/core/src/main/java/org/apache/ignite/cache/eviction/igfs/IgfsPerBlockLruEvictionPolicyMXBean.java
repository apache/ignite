/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.cache.eviction.igfs;

import java.util.Collection;
import org.apache.ignite.mxbean.MXBeanDescription;
import org.jetbrains.annotations.Nullable;

/**
 * MBean for {@code IGFS per-block LRU} eviction policy.
 */
@MXBeanDescription("MBean for IGFS per-block LRU cache eviction policy.")
public interface IgfsPerBlockLruEvictionPolicyMXBean {
    /**
     * Gets maximum allowed size of all blocks in bytes.
     *
     * @return Maximum allowed size of all blocks in bytes.
     */
    @MXBeanDescription("Maximum allowed size of all blocks in bytes.")
    public long getMaxSize();

    /**
     * Sets maximum allowed size of data in all blocks in bytes.
     *
     * @param maxSize Maximum allowed size of data in all blocks in bytes.
     */
    @MXBeanDescription("Sets aximum allowed size of data in all blocks in bytes.")
    public void setMaxSize(long maxSize);

    /**
     * Gets maximum allowed amount of blocks.
     *
     * @return Maximum allowed amount of blocks.
     */
    @MXBeanDescription("Maximum allowed amount of blocks.")
    public int getMaxBlocks();

    /**
     * Sets maximum allowed amount of blocks.
     *
     * @param maxBlocks Maximum allowed amount of blocks.
     */
    @MXBeanDescription("Sets maximum allowed amount of blocks.")
    public void setMaxBlocks(int maxBlocks);

    /**
     * Gets collection of regex for paths whose blocks must not be evicted.
     *
     * @return Collection of regex for paths whose blocks must not be evicted.
     */
    @MXBeanDescription("Collection of regex for paths whose blocks must not be evicted.")
    @Nullable public Collection<String> getExcludePaths();

    /**
     * Sets collection of regex for paths whose blocks must not be evicted.
     *
     * @param excludePaths Collection of regex for paths whose blocks must not be evicted.
     */
    @MXBeanDescription("Sets collection of regex for paths whose blocks must not be evicted.")
    public void setExcludePaths(@Nullable Collection<String> excludePaths);

    /**
     * Gets current size of data in all blocks.
     *
     * @return Current size of data in all blocks.
     */
    @MXBeanDescription("Current size of data in all blocks.")
    public long getCurrentSize();

    /**
     * Gets current amount of blocks.
     *
     * @return Current amount of blocks.
     */
    @MXBeanDescription("Current amount of blocks.")
    public int getCurrentBlocks();
}