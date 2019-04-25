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

package org.apache.ignite.internal.util.offheap;

/**
 * Callback for whenever entries get evicted from off-heap store.
 */
public interface GridOffHeapEvictListener {
    /**
     * Eviction callback.
     *
     * @param part Entry partition
     * @param hash Hash.
     * @param keyBytes Key bytes.
     * @param valBytes Value bytes.
     */
    public void onEvict(int part, int hash, byte[] keyBytes, byte[] valBytes);

    /**
     * @return {@code True} if entry selected for eviction should be immediately removed.
     */
    public boolean removeEvicted();
}