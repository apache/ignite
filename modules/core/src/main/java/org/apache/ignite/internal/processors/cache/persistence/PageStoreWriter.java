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

package org.apache.ignite.internal.processors.cache.persistence;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;

/**
 * Interface for write page to {@link PageStore}.
 */
public interface PageStoreWriter {
    /**
     * Callback for write page. {@link PageMemoryEx} will copy page content to buffer before call.
     *
     * @param fullPageId Page ID to get byte buffer for. The page ID must be present in the collection returned by
     *      the {@link PageMemoryEx#beginCheckpoint(IgniteInternalFuture)} method call.
     * @param buf Temporary buffer to write changes into.
     * @param tag  {@code Partition generation} if data was read, {@code null} otherwise (data already saved to storage).
     * @throws IgniteCheckedException If write page failed.
     */
    void writePage(FullPageId fullPageId, ByteBuffer buf, int tag) throws IgniteCheckedException;
}
