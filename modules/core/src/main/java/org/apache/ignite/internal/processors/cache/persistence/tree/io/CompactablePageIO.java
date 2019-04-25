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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import java.nio.ByteBuffer;

/**
 * Page IO that supports compaction.
 */
public interface CompactablePageIO {
    /**
     * Compacts page contents to the output buffer.
     * Implementation must not change contents, position and limit of the original page buffer.
     *
     * @param page Page buffer.
     * @param out Output buffer.
     * @param pageSize Page size.
     */
    void compactPage(ByteBuffer page, ByteBuffer out, int pageSize);

    /**
     * Restores the original page in place.
     *
     * @param compactPage Compact page.
     * @param pageSize Page size.
     */
    void restorePage(ByteBuffer compactPage, int pageSize);
}
