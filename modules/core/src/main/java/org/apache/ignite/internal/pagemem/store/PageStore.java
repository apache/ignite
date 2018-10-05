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

package org.apache.ignite.internal.pagemem.store;

import org.apache.ignite.IgniteCheckedException;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;

/**
 * Persistent store of pages.
 */
public interface PageStore {
    /**
     * Checks if page exists.
     *
     * @return {@code True} if page exists.
     */
    public boolean exists();

    /**
     * Allocates next page index.
     *
     * @return Next page index.
     * @throws IgniteCheckedException If failed to allocate.
     */
    public long allocatePage() throws IgniteCheckedException;

    /**
     * Gets number of allocated pages.
     *
     * @return Number of allocated pages.
     */
    public int pages();

    /**
     * Reads a page.
     *
     * @param pageId Page ID.
     * @param pageBuf Page buffer to read into.
     * @param keepCrc by default reading zeroes CRC which was on file, but you can keep it in pageBuf if set keepCrc
     * @throws IgniteCheckedException If reading failed (IO error occurred).
     */
    public void read(long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteCheckedException;

    /**
     * Reads a header.
     *
     * @param buf Buffer to write to.
     * @throws IgniteCheckedException If failed.
     */
    public void readHeader(ByteBuffer buf) throws IgniteCheckedException;

    /**
     * Writes a page.
     *
     * @param pageId Page ID.
     * @param pageBuf Page buffer to write.
     * @param tag Partition file version, 1-based incrementing counter. For outdated pages {@code tag} has lower value,
     * and write does nothing.
     * @param calculateCrc if {@code False} crc calculation will be forcibly skipped.
     * @throws IgniteCheckedException If page writing failed (IO error occurred).
     */
    public void write(long pageId, ByteBuffer pageBuf, int tag, boolean calculateCrc) throws IgniteCheckedException;

    /**
     * Gets page offset within the store file.
     *
     * @param pageId Page ID.
     * @return Page offset.
     */
    public long pageOffset(long pageId);

    /**
     * Sync method used to ensure that the given pages are guaranteed to be written to the store.
     *
     * @throws IgniteCheckedException If sync failed (IO error occurred).
     */
    public void sync() throws IgniteCheckedException;

    /**
     * @throws IgniteCheckedException If sync failed (IO error occurred).
     */
    public void ensure() throws IgniteCheckedException;

    /**
     * @return Page store version.
     */
    public int version();

    /**
     * @param cleanFile {@code True} to delete file.
     * @throws StorageException If failed.
     */
    public void stop(boolean cleanFile) throws StorageException;

    /**
     * Starts recover process.
     */
    public void beginRecover();

    /**
     * Ends recover process.
     *
     * @throws StorageException If failed.
     */
    public void finishRecover() throws StorageException;

    /**
     * Truncates and deletes partition file.
     *
     * @param tag New partition tag.
     * @throws StorageException If failed.
     */
    public void truncate(int tag) throws StorageException;
}
