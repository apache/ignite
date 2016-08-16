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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsMetrics;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathSummary;
import org.apache.ignite.igfs.mapreduce.IgfsRecordResolver;
import org.apache.ignite.igfs.mapreduce.IgfsTask;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import java.net.URI;
import java.util.Collection;
import java.util.Map;

/**
 * Mocked IGFS implementation for IGFS tests.
 */
public class IgfsMock implements IgfsEx {
    /** Name. */
    private final String name;

    /**
     * Constructor.
     *
     * @param name Name.
     */
    public IgfsMock(@Nullable String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public IgfsContext context() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgfsPaths proxyPaths() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgfsInputStreamAdapter open(IgfsPath path, int bufSize, int seqReadsBeforePrefetch) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgfsInputStreamAdapter open(IgfsPath path) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgfsInputStreamAdapter open(IgfsPath path, int bufSize) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgfsStatus globalSpace() throws IgniteCheckedException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public void globalSampling(@Nullable Boolean val) throws IgniteCheckedException {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Nullable @Override public Boolean globalSampling() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgfsLocalMetrics localMetrics() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public long groupBlockSize() {
        throwUnsupported();

        return 0;
    }

    /** {@inheritDoc} */
    @Nullable @Override public String clientLogDirectory() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public void clientLogDirectory(String logDir) {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean evictExclude(IgfsPath path, boolean primary) {
        throwUnsupported();

        return false;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid nextAffinityKey() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isProxy(URI path) {
        throwUnsupported();

        return false;
    }

    /** {@inheritDoc} */
    @Override public IgfsSecondaryFileSystem asSecondary() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public FileSystemConfiguration configuration() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgfsPathSummary summary(IgfsPath path) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgfsOutputStream create(IgfsPath path, boolean overwrite) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgfsOutputStream create(IgfsPath path, int bufSize, boolean overwrite, int replication,
        long blockSize, @Nullable Map<String, String> props) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgfsOutputStream create(IgfsPath path, int bufSize, boolean overwrite, @Nullable IgniteUuid affKey,
        int replication, long blockSize, @Nullable Map<String, String> props) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgfsOutputStream append(IgfsPath path, boolean create) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgfsOutputStream append(IgfsPath path, int bufSize, boolean create,
        @Nullable Map<String, String> props) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public void setTimes(IgfsPath path, long accessTime, long modificationTime) throws IgniteException {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsBlockLocation> affinity(IgfsPath path, long start, long len)
        throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsBlockLocation> affinity(IgfsPath path, long start, long len, long maxLen)
        throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgfsMetrics metrics() throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() throws IgniteException {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public long size(IgfsPath path) throws IgniteException {
        throwUnsupported();

        return 0;
    }

    /** {@inheritDoc} */
    @Override public void format() throws IgniteException {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(IgfsTask<T, R> task, @Nullable IgfsRecordResolver rslvr,
        Collection<IgfsPath> paths, @Nullable T arg) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(IgfsTask<T, R> task, @Nullable IgfsRecordResolver rslvr,
        Collection<IgfsPath> paths, boolean skipNonExistentFiles, long maxRangeLen, @Nullable T arg)
        throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends IgfsTask<T, R>> taskCls, @Nullable IgfsRecordResolver rslvr,
        Collection<IgfsPath> paths, @Nullable T arg) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends IgfsTask<T, R>> taskCls, @Nullable IgfsRecordResolver rslvr,
        Collection<IgfsPath> paths, boolean skipNonExistentFiles, long maxRangeLen, @Nullable T arg)
        throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean exists(IgfsPath path) {
        throwUnsupported();

        return false;
    }

    /** {@inheritDoc} */
    @Override public IgfsFile update(IgfsPath path, Map<String, String> props) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public void rename(IgfsPath src, IgfsPath dest) throws IgniteException {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public boolean delete(IgfsPath path, boolean recursive) throws IgniteException {
        throwUnsupported();

        return false;
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path) throws IgniteException {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgfsPath path, @Nullable Map<String, String> props) throws IgniteException {
        throwUnsupported();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsPath> listPaths(IgfsPath path) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsFile> listFiles(IgfsPath path) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgfsFile info(IgfsPath path) throws IgniteException {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgfsMode mode(IgfsPath path) {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public long usedSpaceSize() throws IgniteException {
        throwUnsupported();

        return 0;
    }

    /** {@inheritDoc} */
    @Override public IgniteFileSystem withAsync() {
        throwUnsupported();

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        throwUnsupported();

        return false;
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future() {
        throwUnsupported();

        return null;
    }

    /**
     * Throw {@link UnsupportedOperationException}.
     */
    private static void throwUnsupported() {
        throw new UnsupportedOperationException("Should not be called!");
    }
}
