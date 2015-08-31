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

import java.net.URI;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsMetrics;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathSummary;
import org.apache.ignite.igfs.mapreduce.IgfsRecordResolver;
import org.apache.ignite.igfs.mapreduce.IgfsTask;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.AsyncSupportAdapter;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Igfs supporting asynchronous operations.
 */
public class IgfsAsyncImpl extends AsyncSupportAdapter<IgniteFileSystem> implements IgfsEx {
    /** */
    private final IgfsImpl igfs;

    /**
     * @param igfs Igfs.
     */
    public IgfsAsyncImpl(IgfsImpl igfs) {
        super(true);

        this.igfs = igfs;
    }

    /** {@inheritDoc} */
    @Override public void format() {
        try {
            saveOrGet(igfs.formatAsync());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(IgfsTask<T, R> task, @Nullable IgfsRecordResolver rslvr,
        Collection<IgfsPath> paths, @Nullable T arg) {
        try {
            return saveOrGet(igfs.executeAsync(task, rslvr, paths, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(IgfsTask<T, R> task, @Nullable IgfsRecordResolver rslvr,
        Collection<IgfsPath> paths, boolean skipNonExistentFiles, long maxRangeLen, @Nullable T arg) {
        try {
            return saveOrGet(igfs.executeAsync(task, rslvr, paths, skipNonExistentFiles, maxRangeLen, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends IgfsTask<T, R>> taskCls,
        @Nullable IgfsRecordResolver rslvr, Collection<IgfsPath> paths, @Nullable T arg) {
        try {
            return saveOrGet(igfs.executeAsync(taskCls, rslvr, paths, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends IgfsTask<T, R>> taskCls,
        @Nullable IgfsRecordResolver rslvr, Collection<IgfsPath> paths, boolean skipNonExistentFiles,
        long maxRangeLen, @Nullable T arg) {
        try {
            return saveOrGet(igfs.executeAsync(taskCls, rslvr, paths, skipNonExistentFiles, maxRangeLen, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        igfs.stop(cancel);
    }

    /** {@inheritDoc} */
    @Override public IgfsContext context() {
        return igfs.context();
    }

    /** {@inheritDoc} */
    @Override public IgfsPaths proxyPaths() {
        return igfs.proxyPaths();
    }

    /** {@inheritDoc} */
    @Override public IgfsInputStreamAdapter open(IgfsPath path, int bufSize,
        int seqReadsBeforePrefetch) {
        return igfs.open(path, bufSize, seqReadsBeforePrefetch);
    }

    /** {@inheritDoc} */
    @Override public IgfsInputStreamAdapter open(IgfsPath path) {
        return igfs.open(path);
    }

    /** {@inheritDoc} */
    @Override public IgfsInputStreamAdapter open(IgfsPath path, int bufSize) {
        return igfs.open(path, bufSize);
    }

    /** {@inheritDoc} */
    @Override public IgfsStatus globalSpace() throws IgniteCheckedException {
        return igfs.globalSpace();
    }

    /** {@inheritDoc} */
    @Override public void globalSampling(@Nullable Boolean val) throws IgniteCheckedException {
        igfs.globalSampling(val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Boolean globalSampling() {
        return igfs.globalSampling();
    }

    /** {@inheritDoc} */
    @Override public IgfsLocalMetrics localMetrics() {
        return igfs.localMetrics();
    }

    /** {@inheritDoc} */
    @Override public long groupBlockSize() {
        return igfs.groupBlockSize();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> awaitDeletesAsync() throws IgniteCheckedException {
        return igfs.awaitDeletesAsync();
    }

    /** {@inheritDoc} */
    @Nullable @Override public String clientLogDirectory() {
        return igfs.clientLogDirectory();
    }

    /** {@inheritDoc} */
    @Override public void clientLogDirectory(String logDir) {
        igfs.clientLogDirectory(logDir);
    }

    /** {@inheritDoc} */
    @Override public boolean evictExclude(IgfsPath path, boolean primary) {
        return igfs.evictExclude(path, primary);
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid nextAffinityKey() {
        return igfs.nextAffinityKey();
    }

    /** {@inheritDoc} */
    @Override public boolean isProxy(URI path) {
        return igfs.isProxy(path);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String name() {
        return igfs.name();
    }

    /** {@inheritDoc} */
    @Override public FileSystemConfiguration configuration() {
        return igfs.configuration();
    }

    /** {@inheritDoc} */
    @Override public IgfsPathSummary summary(IgfsPath path) {
        return igfs.summary(path);
    }

    /** {@inheritDoc} */
    @Override public IgfsOutputStream create(IgfsPath path, boolean overwrite) {
        return igfs.create(path, overwrite);
    }

    /** {@inheritDoc} */
    @Override public IgfsOutputStream create(IgfsPath path, int bufSize, boolean overwrite, int replication,
        long blockSize, @Nullable Map<String, String> props) {
        return igfs.create(path, bufSize, overwrite, replication, blockSize, props);
    }

    /** {@inheritDoc} */
    @Override public IgfsOutputStream create(IgfsPath path, int bufSize, boolean overwrite,
        @Nullable IgniteUuid affKey, int replication, long blockSize, @Nullable Map<String, String> props) {
        return igfs.create(path, bufSize, overwrite, affKey, replication, blockSize, props);
    }

    /** {@inheritDoc} */
    @Override public IgfsOutputStream append(IgfsPath path, boolean create) {
        return igfs.append(path, create);
    }

    /** {@inheritDoc} */
    @Override public IgfsOutputStream append(IgfsPath path, int bufSize, boolean create,
        @Nullable Map<String, String> props) {
        return igfs.append(path, bufSize, create, props);
    }

    /** {@inheritDoc} */
    @Override public void setTimes(IgfsPath path, long accessTime, long modificationTime) {
        igfs.setTimes(path, accessTime, modificationTime);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsBlockLocation> affinity(IgfsPath path, long start, long len) {
        return igfs.affinity(path, start, len);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsBlockLocation> affinity(IgfsPath path, long start, long len, long maxLen) {
        return igfs.affinity(path, start, len, maxLen);
    }

    /** {@inheritDoc} */
    @Override public IgfsMetrics metrics() {
        return igfs.metrics();
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        igfs.resetMetrics();
    }

    /** {@inheritDoc} */
    @Override public long size(IgfsPath path) {
        return igfs.size(path);
    }

    /** {@inheritDoc} */
    @Override public boolean exists(IgfsPath path) {
        return igfs.exists(path);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgfsFile update(IgfsPath path, Map<String, String> props) {
        return igfs.update(path, props);
    }

    /** {@inheritDoc} */
    @Override public void rename(IgfsPath src, IgfsPath dest) {
        igfs.rename(src, dest);
    }

    /** {@inheritDoc} */
    @Override public boolean delete(IgfsPath path, boolean recursive) {
        return igfs.delete(path, recursive);
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path) {
        igfs.mkdirs(path);
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path, @Nullable Map<String, String> props) {
        igfs.mkdirs(path, props);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsPath> listPaths(IgfsPath path) {
        return igfs.listPaths(path);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsFile> listFiles(IgfsPath path) {
        return igfs.listFiles(path);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgfsFile info(IgfsPath path) {
        return igfs.info(path);
    }

    /** {@inheritDoc} */
    @Override public long usedSpaceSize() {
        return igfs.usedSpaceSize();
    }

    /** {@inheritDoc} */
    @Override public IgfsSecondaryFileSystem asSecondary() {
        return igfs.asSecondary();
    }
}