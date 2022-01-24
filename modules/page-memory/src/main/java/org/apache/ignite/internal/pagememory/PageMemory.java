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

package org.apache.ignite.internal.pagememory;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;

/**
 * Class responsible for pages storage and handling.
 */
//TODO IGNITE-16350 Improve javadoc in this class.
public interface PageMemory extends PageIdAllocator, PageSupport {
    /**
     * Returns a page's size in bytes.
     */
    int pageSize();

    /**
     * Returns a page size without the encryption overhead, in bytes.
     *
     * @param groupId Group id.
     */
    //TODO IGNITE-16350 Consider renaming.
    int realPageSize(int groupId);

    /**
     * Returns a page's size with system overhead, in bytes.
     */
    //TODO IGNITE-16350 Consider renaming.
    int systemPageSize();

    /**
     * Wraps a page address, obtained by a {@code readLock}/{@code writeLock} or their variants, into a direct byte buffer.
     *
     * @param pageAddr Page address.
     * @return Page byte buffer.
     */
    ByteBuffer pageBuffer(long pageAddr);

    /**
     * Returns the total number of pages loaded into memory.
     */
    long loadedPages();

    /**
     * Returns a registry to obtain {@link PageIo} instances for pages.
     */
    public PageIoRegistry ioRegistry();
}
