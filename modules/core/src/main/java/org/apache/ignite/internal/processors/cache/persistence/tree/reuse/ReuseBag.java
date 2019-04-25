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

/**
 * Reuse bag for free index pages.
 */
public interface ReuseBag {
    /**
     * @param pageId Free page ID for reuse.
     */
    public void addFreePage(long pageId);

    /**
     * @return Free page ID for reuse or {@code 0} if empty.
     */
    public long pollFreePage();

    /**
     * @return {@code true} if no contained page IDs for reuse.
     */
    public boolean isEmpty();
}
