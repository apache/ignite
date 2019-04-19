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
package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.FullPageId;

/**
 * Flush (write) dirty page implementation for freed page during page replacement. When possible, will be called by
 * removePageForReplacement().
 */
public interface ReplacedPageWriter {
    /**
     * @param fullPageId Full page ID being evicted.
     * @param byteBuf Buffer with page data.
     * @param tag partition update tag, increasing counter.
     * @throws IgniteCheckedException if page write failed.
     */
    void writePage(FullPageId fullPageId, ByteBuffer byteBuf, int tag) throws IgniteCheckedException;
}
