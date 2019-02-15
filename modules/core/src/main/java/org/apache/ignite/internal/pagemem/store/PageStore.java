/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.pagemem.store;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
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

    /**
     * @return Page size in bytes.
     */
    public int getPageSize();

    /**
     * @return Storage block size or negative value if unknown or not supported.
     */
    public int getBlockSize();

    /**
     * @return Size of the storage in bytes. May differ from {@link #pages()} * {@link #getPageSize()}
     *         due to delayed writes or due to other implementation specific details.
     */
    public long size();

    /**
     * @return Size of the storage adjusted for sparsity in bytes or negative
     *         value if not supported. Should be less than or equal to {@link #size()}.
     * @see #punchHole
     */
    public long getSparseSize();

    /**
     * Should free all the extra storage space after the given number of useful bytes in the given page.
     *
     * @param pageId Page id.
     * @param usefulBytes Number of meaningful bytes from the beginning of the page.
     */
    void punchHole(long pageId, int usefulBytes);
}
