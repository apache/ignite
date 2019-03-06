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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.IgniteException;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystemPositionedReadable;
import org.jetbrains.annotations.Nullable;

import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;

/**
 * Secondary file system over native IGFS.
 */
class IgfsSecondaryFileSystemImpl implements IgfsSecondaryFileSystem {
    /** Delegate. */
    private final IgfsEx igfs;

    /**
     * Constructor.
     *
     * @param igfs Delegate.
     */
    IgfsSecondaryFileSystemImpl(IgfsEx igfs) {
        this.igfs = igfs;
    }

    /** {@inheritDoc} */
    @Override public boolean exists(IgfsPath path) {
        return igfs.exists(path);
    }

    /** {@inheritDoc} */
    @Override public IgfsFile update(IgfsPath path, Map<String, String> props) throws IgniteException {
        return igfs.update(path, props);
    }

    /** {@inheritDoc} */
    @Override public void rename(IgfsPath src, IgfsPath dest) throws IgniteException {
        igfs.rename(src, dest);
    }

    /** {@inheritDoc} */
    @Override public boolean delete(IgfsPath path, boolean recursive) throws IgniteException {
        return igfs.delete(path, recursive);
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path) throws IgniteException {
        igfs.mkdirs(path);
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path, @Nullable Map<String, String> props) throws IgniteException {
        igfs.mkdirs(path, props);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsPath> listPaths(IgfsPath path) throws IgniteException {
        return igfs.listPaths(path);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsFile> listFiles(IgfsPath path) throws IgniteException {
        return igfs.listFiles(path);
    }

    /** {@inheritDoc} */
    @Override public IgfsSecondaryFileSystemPositionedReadable open(IgfsPath path, int bufSize)
        throws IgniteException {
        return (IgfsSecondaryFileSystemPositionedReadable)igfs.open(path, bufSize);
    }

    /** {@inheritDoc} */
    @Override public OutputStream create(IgfsPath path, boolean overwrite) throws IgniteException {
        return igfs.create(path, overwrite);
    }

    /** {@inheritDoc} */
    @Override public OutputStream create(IgfsPath path, int bufSize, boolean overwrite, int replication,
        long blockSize, @Nullable Map<String, String> props) throws IgniteException {
        return igfs.create(path, bufSize, overwrite, replication, blockSize, props);
    }

    /** {@inheritDoc} */
    @Override public OutputStream append(IgfsPath path, int bufSize, boolean create,
        @Nullable Map<String, String> props) throws IgniteException {
        return igfs.append(path, bufSize, create, props);
    }

    /** {@inheritDoc} */
    @Override public IgfsFile info(IgfsPath path) throws IgniteException {
        return igfs.info(path);
    }

    /** {@inheritDoc} */
    @Override public long usedSpaceSize() throws IgniteException {
        return igfs.usedSpaceSize();
    }

    /** {@inheritDoc} */
    @Override public void setTimes(IgfsPath path, long modificationTime, long accessTime) throws IgniteException {
        igfs.setTimes(path, modificationTime, accessTime);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsBlockLocation> affinity(IgfsPath path, long start, long len,
        long maxLen) throws IgniteException {
        return igfs.affinity(path, start, len, maxLen);
    }
}