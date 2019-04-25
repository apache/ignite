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

package org.apache.ignite.internal.processors.cache.persistence.tree.reuse;

import org.apache.ignite.IgniteCheckedException;

/**
 * Reuse list.
 */
public interface ReuseList {
    /**
     * @param bag Reuse bag.
     * @throws IgniteCheckedException If failed.
     */
    public void addForRecycle(ReuseBag bag) throws IgniteCheckedException;

    /**
     * @return Page ID or {@code 0} if none available.
     * @throws IgniteCheckedException If failed.
     */
    public long takeRecycledPage() throws IgniteCheckedException;

    /**
     * @return Number of recycled pages it contains.
     * @throws IgniteCheckedException If failed.
     */
    public long recycledPagesCount() throws IgniteCheckedException;
}
