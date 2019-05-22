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

package org.apache.ignite.internal.processors.cache.persistence.backup;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;

/**
 * Backup store of pages for particular cache partition file.
 */
public interface TemporaryStore extends AutoCloseable {
    /**
     * @param pageBuf Page buffer to read into.
     * @throws IgniteCheckedException If failed (IO error occurred).
     */
    public void read(ByteBuffer pageBuf) throws IgniteCheckedException;

    /**
     * Write a page to store.
     *
     * @param pageId Page ID.
     * @param pageBuf Page buffer to write.
     * @throws IgniteCheckedException If page writing failed (IO error occurred).
     */
    public void write(long pageId, ByteBuffer pageBuf) throws IgniteCheckedException;

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void truncate() throws IgniteCheckedException;

    /**
     * @return The value of pages successfully written to the temporary store.
     */
    public int writtenPagesCount();
}
