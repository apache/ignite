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

package org.apache.ignite.internal.pagemem;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.lifecycle.LifecycleAware;

import java.nio.ByteBuffer;
import java.util.Collection;

/**
 */
public interface PageMemory extends LifecycleAware, PageIdAllocator {
    /**
     * @return Meta page.
     */
    public Page metaPage() throws IgniteCheckedException;

    /**
     * Gets the page associated with the given page ID. Each page obtained with this method must be released by
     * calling {@link #releasePage(Page)}.
     *
     * @param pageId Page ID.
     * @return Page or {@code null} if this page ID was not allocated.
     */
    public Page page(FullPageId pageId) throws IgniteCheckedException;

    /**
     * @param page Page to release.
     */
    public void releasePage(Page page) throws IgniteCheckedException;

    /**
     * @return Page size in bytes.
     */
    public int pageSize();

    /**
     * Gets a collection of dirty page IDs since the last checkpoint. If a dirty page is being written after
     * the checkpointing operation begun, the modifications will be written to a temporary buffer which will
     * be flushed to the main memory after the checkpointing finished. This method must be called when no
     * concurrent operations on pages are performed.
     *
     * @return Collection of dirty page IDs.
     * @throws IgniteException If checkpoint has been already started and was not finished.
     */
    public Collection<FullPageId> beginCheckpoint() throws IgniteException;

    /**
     * Finishes checkpoint operation.
     */
    public void finishCheckpoint();

    /**
     * Gets page byte buffer for the checkpoint procedure.
     *
     * @param pageId Page ID to get byte buffer for. The page ID must be present in the collection returned by
     *      the {@link #beginCheckpoint()} method call.
     * @param tmpBuf Temporary buffer to write changes into.
     * @return {@code True} if data were read, {@code false} otherwise (data already saved to storage).
     * @throws IgniteException If failed to obtain page data.
     */
    public boolean getForCheckpoint(FullPageId pageId, ByteBuffer tmpBuf);
}
