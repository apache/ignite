/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.*;
import org.apache.ignite.fs.*;
import org.apache.ignite.fs.mapreduce.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.jetbrains.annotations.*;

import java.net.*;
import java.util.*;

/**
 * Ggfs supporting asynchronous operations.
 */
public class GridGgfsAsyncImpl extends IgniteAsyncSupportAdapter implements GridGgfsEx {
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
    @Override public IgniteFs enableAsync() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void format() throws IgniteCheckedException {
        saveOrGet(ggfs.formatAsync());
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(IgniteFsTask<T, R> task, @Nullable IgniteFsRecordResolver rslvr,
        Collection<IgniteFsPath> paths, @Nullable T arg) throws IgniteCheckedException {
        return saveOrGet(ggfs.executeAsync(task, rslvr, paths, arg));
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(IgniteFsTask<T, R> task, @Nullable IgniteFsRecordResolver rslvr,
        Collection<IgniteFsPath> paths, boolean skipNonExistentFiles, long maxRangeLen, @Nullable T arg)
        throws IgniteCheckedException {
        return saveOrGet(ggfs.executeAsync(task, rslvr, paths, skipNonExistentFiles, maxRangeLen, arg));
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends IgniteFsTask<T, R>> taskCls,
        @Nullable IgniteFsRecordResolver rslvr, Collection<IgniteFsPath> paths, @Nullable T arg) throws IgniteCheckedException {
        return saveOrGet(ggfs.executeAsync(taskCls, rslvr, paths, arg));
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends IgniteFsTask<T, R>> taskCls,
        @Nullable IgniteFsRecordResolver rslvr, Collection<IgniteFsPath> paths, boolean skipNonExistentFiles,
        long maxRangeLen, @Nullable T arg) throws IgniteCheckedException {
        return saveOrGet(ggfs.executeAsync(taskCls, rslvr, paths, skipNonExistentFiles, maxRangeLen, arg));
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
        int seqReadsBeforePrefetch) throws IgniteCheckedException {
        return ggfs.open(path, bufSize, seqReadsBeforePrefetch);
    }

    /** {@inheritDoc} */
    @Override public GridGgfsInputStreamAdapter open(IgniteFsPath path) throws IgniteCheckedException {
        return ggfs.open(path);
    }

    /** {@inheritDoc} */
    @Override public GridGgfsInputStreamAdapter open(IgniteFsPath path, int bufSize) throws IgniteCheckedException {
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
    @Override public IgniteFuture<?> awaitDeletesAsync() throws IgniteCheckedException {
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
    @Override public IgniteFsPathSummary summary(IgniteFsPath path) throws IgniteCheckedException {
        return ggfs.summary(path);
    }

    /** {@inheritDoc} */
    @Override public IgniteFsOutputStream create(IgniteFsPath path, boolean overwrite) throws IgniteCheckedException {
        return ggfs.create(path, overwrite);
    }

    /** {@inheritDoc} */
    @Override public IgniteFsOutputStream create(IgniteFsPath path, int bufSize, boolean overwrite, int replication,
        long blockSize, @Nullable Map<String, String> props) throws IgniteCheckedException {
        return ggfs.create(path, bufSize, overwrite, replication, blockSize, props);
    }

    /** {@inheritDoc} */
    @Override public IgniteFsOutputStream create(IgniteFsPath path, int bufSize, boolean overwrite,
        @Nullable IgniteUuid affKey, int replication, long blockSize, @Nullable Map<String, String> props)
        throws IgniteCheckedException {
        return ggfs.create(path, bufSize, overwrite, affKey, replication, blockSize, props);
    }

    /** {@inheritDoc} */
    @Override public IgniteFsOutputStream append(IgniteFsPath path, boolean create) throws IgniteCheckedException {
        return ggfs.append(path, create);
    }

    /** {@inheritDoc} */
    @Override public IgniteFsOutputStream append(IgniteFsPath path, int bufSize, boolean create,
        @Nullable Map<String, String> props) throws IgniteCheckedException {
        return ggfs.append(path, bufSize, create, props);
    }

    /** {@inheritDoc} */
    @Override public void setTimes(IgniteFsPath path, long accessTime, long modificationTime) throws IgniteCheckedException {
        ggfs.setTimes(path, accessTime, modificationTime);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsBlockLocation> affinity(IgniteFsPath path, long start, long len)
        throws IgniteCheckedException {
        return ggfs.affinity(path, start, len);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsBlockLocation> affinity(IgniteFsPath path, long start, long len, long maxLen)
        throws IgniteCheckedException {
        return ggfs.affinity(path, start, len, maxLen);
    }

    /** {@inheritDoc} */
    @Override public IgniteFsMetrics metrics() throws IgniteCheckedException {
        return ggfs.metrics();
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() throws IgniteCheckedException {
        ggfs.resetMetrics();
    }

    /** {@inheritDoc} */
    @Override public long size(IgniteFsPath path) throws IgniteCheckedException {
        return ggfs.size(path);
    }

    /** {@inheritDoc} */
    @Override public boolean exists(IgniteFsPath path) throws IgniteCheckedException {
        return ggfs.exists(path);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFsFile update(IgniteFsPath path, Map<String, String> props) throws IgniteCheckedException {
        return ggfs.update(path, props);
    }

    /** {@inheritDoc} */
    @Override public void rename(IgniteFsPath src, IgniteFsPath dest) throws IgniteCheckedException {
        ggfs.rename(src, dest);
    }

    /** {@inheritDoc} */
    @Override public boolean delete(IgniteFsPath path, boolean recursive) throws IgniteCheckedException {
        return ggfs.delete(path, recursive);
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgniteFsPath path) throws IgniteCheckedException {
        ggfs.mkdirs(path);
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(IgniteFsPath path, @Nullable Map<String, String> props) throws IgniteCheckedException {
        ggfs.mkdirs(path, props);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsPath> listPaths(IgniteFsPath path) throws IgniteCheckedException {
        return ggfs.listPaths(path);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFsFile> listFiles(IgniteFsPath path) throws IgniteCheckedException {
        return ggfs.listFiles(path);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFsFile info(IgniteFsPath path) throws IgniteCheckedException {
        return ggfs.info(path);
    }

    /** {@inheritDoc} */
    @Override public long usedSpaceSize() throws IgniteCheckedException {
        return ggfs.usedSpaceSize();
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> properties() {
        return ggfs.properties();
    }
}
