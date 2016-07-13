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

import java.nio.ByteBuffer;

/**
 *
 */
public interface Page extends AutoCloseable {
    /**
     * Gets the page ID. Page ID is a unique page identifier that does not change when partitions migrate
     * from one node to another. Links (which is a page ID and 8-byte offset within a page) must be used
     * when referencing data across pages.
     *
     * @return Page ID.
     */
    public long id();

    /**
     * @return Full page ID.
     */
    public FullPageId fullId();

    /**
     * @return ByteBuffer for modifying the page.
     */
    public ByteBuffer getForRead();

    /**
     * Releases reserved page. Released page can be evicted from RAM after flushing modifications to disk.
     */
    public void releaseRead();

    /**
     * @return ByteBuffer for modifying the page.
     */
    public ByteBuffer getForWrite();

    /**
     * Releases reserved page. Released page can be evicted from RAM after flushing modifications to disk.
     */
    public void releaseWrite(boolean markDirty);

    /**
     * @return {@code True} if the page was modified since the last checkpoint.
     */
    public boolean isDirty();

    /**
     * Release page.
     */
    @Override public void close();
}
