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

package org.apache.ignite.internal.processors.cache.database.tree.util;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagemem.Page;

/**
 * Page lock listener.
 */
public interface PageLockListener {
    /**
     * @param page Page.
     */
    public void onBeforeWriteLock(Page page);

    /**
     * @param page Page.
     * @param buf Buffer or {@code null} if attempt to lock failed.
     */
    public void onWriteLock(Page page, ByteBuffer buf);

    /**
     * @param page Page.
     * @param buf Buffer.
     */
    public void onWriteUnlock(Page page, ByteBuffer buf);

    /**
     * @param page Page.
     */
    public void onBeforeReadLock(Page page);

    /**
     * @param page Page.
     * @param buf Buffer or {@code null} if attempt to lock failed.
     */
    public void onReadLock(Page page, ByteBuffer buf);

    /**
     * @param page Page.
     * @param buf Buffer.
     */
    public void onReadUnlock(Page page, ByteBuffer buf);
}
