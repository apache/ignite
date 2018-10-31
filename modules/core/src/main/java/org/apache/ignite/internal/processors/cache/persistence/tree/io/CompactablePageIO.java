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
