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
import org.apache.ignite.lifecycle.LifecycleAware;

/**
 */
public interface PageMemory extends LifecycleAware, PageIdAllocator, PageSupport {
    /**
     * @return Page size in bytes.
     */
    public int pageSize();

    /**
     * @return Page size with system overhead, in bytes.
     */
    public int systemPageSize();

    /**
     * @param pageAddr Page address.
     * @return Page byte buffer.
     */
    public ByteBuffer pageBuffer(long pageAddr);

    /**
     * @return Total number of loaded pages in memory.
     */
    public long loadedPages();

    /**
     * Number of pages used in checkpoint buffer.
     */
    public int checkpointBufferPagesCount();
}
