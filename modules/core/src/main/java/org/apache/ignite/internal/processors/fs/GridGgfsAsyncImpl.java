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

package org.apache.ignite.internal.processors.fs;

import org.apache.ignite.*;
import org.apache.ignite.fs.*;
import org.apache.ignite.fs.mapreduce.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.util.*;

/**
 * Ggfs supporting asynchronous operations.
 */
public class GridGgfsAsyncImpl extends AsyncSupportAdapter<IgniteFs> implements GridGgfsEx {
    /** */
    private final GridGgfsImpl ggfs;

    /**
     * @param ggfs Ggfs.
     */
    public GridGgfsAsyncImpl(GridGgfsImpl ggfs) {
        super(true);

        this.ggfs = ggfs;
    }

    /** {@inheritDoc} */
    @Override public void format() {
        try {
            saveOrGet(ggfs.formatAsync());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(IgniteFsTask<T, R> task, @Nullable IgniteFsRecordResolver rslvr,
        Collection<IgniteFsPath> paths, @Nullable T arg) {
        try {
            return saveOrGet(ggfs.executeAsync(task, rslvr, paths, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(IgniteFsTask<T, R> task, @Nullable IgniteFsRecordResolver rslvr,
        Collection<IgniteFsPath> paths, boolean skipNonExistentFiles, long maxRangeLen, @Nullable T arg) {
        try {
            return saveOrGet(ggfs.executeAsync(task, rslvr, paths, skipNonExistentFiles, maxRangeLen, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends IgniteFsTask<T, R>> taskCls,
        @Nullable IgniteFsRecordResolver rslvr, Collection<IgniteFsPath> paths, @Nullable T arg) {
        try {
            return saveOrGet(ggfs.executeAsync(taskCls, rslvr, paths, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends IgniteFsTask<T, R>> taskCls,
        @Nullable IgniteFsRecordResolver rslvr, Collection<IgniteFsPath> paths, boolean skipNonExistentFiles,
        long maxRangeLen, @Nullable T arg) {
        try {
            return saveOrGet(ggfs.executeAsync(taskCls, rslvr, paths, skipNonExistentFiles, maxRangeLen, arg));
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        ggfs.stop();
    }

    /** {@inheritDoc} */
    @Override public GridGgfsContext context() {
        return ggfs.context();
    }

    /** {@inheritDoc} */
    @Override public GridGgfsPaths proxyPaths() {
        return ggfs.proxyPaths();
    }

    /** {@inheritDoc} */
    @Override public GridGgfsInputStreamAdapter open(IgniteFsPath path, int bufSize,
        int seqReadsBeforePrefetch) {
        return ggfs.open(path, bufSize, seqReadsBeforePrefetch);
    }

    /** {@inheritDoc} */
    @Override public GridGgfsInputStreamAdapter open(IgniteFsPath path) {
        return ggfs.open(path);
    }

    /** {@inheritDoc} */
    @Override public GridGgfsInputStreamAdapter open(IgniteFsPath path, int bufSize) {
        return ggfs.open(path, bufSize);
    }

    /** {@inheritDoc} */
    @Override public GridGgfsStatus globalSpace() throws IgniteCheckedException {
        return ggfs.globalSpace();
    }

    /** {@inheritDoc} */
    @Override public void globalSampling(@Nullable Boolean val) throws IgniteCheckedException {
        ggfs.globalSampling(val);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Boolean globalSampling() {
        return ggfs.globalSampling();
    }

    /** {@inheritDoc} */
    @Override public GridGgfsLocalMetrics localMetrics() {
        return ggfs.localMetrics();
    }

    /** {@inheritDoc} */
    @Override public long groupBlockSize() {
        return ggfs.groupBlockSize();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> awaitDeletesAsync() throws IgniteCheckedException {
        return ggfs.awaitDeletesAsync();
    }

    /** {@inheritDoc} */
    @Nullable @Override public String clientLogDirectory() {
        return ggfs.clientLogDirectory();
    }

    /** {@inheritDoc} */
    @Override public void clientLogDirectory(String logDir) {
        ggfs.clientLogDirectory(logDir);
    }

    /** {@inheritDoc} */
    @Override public boolean evictExclude(IgniteFsPath path, boolean primary) {
        return ggfs.evictExclude(path, primary);
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid nextAffinityKey() {
        return ggfs.nextAffinityKey();
    }

    /** {@inheritDoc} */
    @Override public boolean isProxy(URI path) {
        return ggfs.isProxy(path);
    }

    /** {@inheritDoc} */
    @Nullable @Override public String name() {
        return ggfs.name();
    }

    /** {@inheritDoc} */
    @Override public IgniteFsConfiguration configuration() {
        return ggfs.configuration();
    }

    /** {@inheritDoc} */
    @Override public IgniteFsPathSummary summary(IgniteFsPath path) {
        return ggfs.summary(path);
    }

    /** {@inheritDoc} */
    @Override public IgniteFsOutputStream create(IgniteFsPath path, boolean overwrite) {
        return ggfs.create(path, overwrite);
    }

    /** {@inheritDoc} */
    @Override public IgniteFsOutputStream create(IgniteFsPath path, int bufSize, boolean overwrite, int replication,
        long blockSize, @Nullable Map<String, String> props) {
        return ggfs.create(path, bufSize, overwrite, replication, blockSize, props);
    }

    /** {@inheritDoc} */
    @Override public IgniteFsOutputStream create(IgniteFsPath path, int bufSize, boolean overwrite,
        @Nullable IgniteUuid affKey, int replication, long blockSize, @Nullable Map<String, String> props) {
        return ggfs.create(path, bufSize, overwrite, affKey, replication, blockSize, props);
    }

    /** {@inheritDoc} */
    @Override public IgniteFsOutputStream append(IgniteFsPath path, boolean create) {
        return ggfs.append(path, create);
    }

    /** {@inheritDoc} */
    @Override public IgniteFsOutputStream append(IgniteFsPath path, int bufSize, boolean create,
        @Nullable Map<String, String> props) {
        return ggfs.append(path, bufSize, create, props);
    }

    /** {@inheritDoc} */
    @Override public void setTimes(IgniteFsPath path, long accessTime, long modificationTime) {
        ggfs.setTimes(path, accessTime, modificationTime);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsBlockLocation> affinity(IgniteFsPath path, long start, long len) {
        return ggfs.affinity(path, start, len);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsBlockLocation> affinity(IgniteFsPath path, long start, long len, long maxLen) {
        return ggfs.affinity(path, start, len, maxLen);
    }

    /** {@inheritDoc} */
    @Override public IgniteFsMetrics metrics() {
        return ggfs.metrics();
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        ggfs.resetMetrics();
    }

    /** {@inheritDoc} */
    @Override public long size(IgniteFsPath path) {
        return ggfs.size(path);
    }

    /** {@inheritDoc} */
    @Override public boolean exists(IgniteFsPath path) {
        return ggfs.exists(path);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFsFile update(IgniteFsPath path, Map<String, String> props) {
        return ggfs.update(path, props);
    }

    /** {@inheritDoc} */
    @Override public void rename(IgniteFsPath src, IgniteFsPath dest) {
        ggfs.rename(src, dest);
    }

    /** {@inheritDoc} */
    @Override public boolean delete(IgniteFsPath path, boolean recursive) {
        return ggfs.delete(path, recursive);
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgniteFsPath path) {
        ggfs.mkdirs(path);
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgniteFsPath path, @Nullable Map<String, String> props) {
        ggfs.mkdirs(path, props);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsPath> listPaths(IgniteFsPath path) {
        return ggfs.listPaths(path);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsFile> listFiles(IgniteFsPath path) {
        return ggfs.listFiles(path);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFsFile info(IgniteFsPath path) {
        return ggfs.info(path);
    }

    /** {@inheritDoc} */
    @Override public long usedSpaceSize() {
        return ggfs.usedSpaceSize();
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> properties() {
        return ggfs.properties();
    }
}
