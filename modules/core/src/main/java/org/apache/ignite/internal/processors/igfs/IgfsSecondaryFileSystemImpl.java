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

package org.apache.ignite.internal.processors.igfs;

import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystemPositionedReadable;
import org.jetbrains.annotations.Nullable;

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
        return igfs.open(path, bufSize);
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
    @Override public Map<String, String> properties() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteException {
        // No-op.
    }
}