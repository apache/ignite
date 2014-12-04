/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.ggfs.mapreduce.*;
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
    @Override public GridGgfs enableAsync() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public void format() throws GridException {
        saveOrGet(ggfs.formatAsync());
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(GridGgfsTask<T, R> task, @Nullable GridGgfsRecordResolver rslvr,
        Collection<GridGgfsPath> paths, @Nullable T arg) throws GridException {
        return saveOrGet(ggfs.executeAsync(task, rslvr, paths, arg));
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(GridGgfsTask<T, R> task, @Nullable GridGgfsRecordResolver rslvr,
        Collection<GridGgfsPath> paths, boolean skipNonExistentFiles, long maxRangeLen, @Nullable T arg)
        throws GridException {
        return saveOrGet(ggfs.executeAsync(task, rslvr, paths, skipNonExistentFiles, maxRangeLen, arg));
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends GridGgfsTask<T, R>> taskCls,
        @Nullable GridGgfsRecordResolver rslvr, Collection<GridGgfsPath> paths, @Nullable T arg) throws GridException {
        return saveOrGet(ggfs.executeAsync(taskCls, rslvr, paths, arg));
    }

    /** {@inheritDoc} */
    @Override public <T, R> R execute(Class<? extends GridGgfsTask<T, R>> taskCls,
        @Nullable GridGgfsRecordResolver rslvr, Collection<GridGgfsPath> paths, boolean skipNonExistentFiles,
        long maxRangeLen, @Nullable T arg) throws GridException {
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
    @Override public GridGgfsInputStreamAdapter open(GridGgfsPath path, int bufSize,
        int seqReadsBeforePrefetch) throws GridException {
        return ggfs.open(path, bufSize, seqReadsBeforePrefetch);
    }

    /** {@inheritDoc} */
    @Override public GridGgfsInputStreamAdapter open(GridGgfsPath path) throws GridException {
        return ggfs.open(path);
    }

    /** {@inheritDoc} */
    @Override public GridGgfsInputStreamAdapter open(GridGgfsPath path, int bufSize) throws GridException {
        return ggfs.open(path, bufSize);
    }

    /** {@inheritDoc} */
    @Override public GridGgfsStatus globalSpace() throws GridException {
        return ggfs.globalSpace();
    }

    /** {@inheritDoc} */
    @Override public void globalSampling(@Nullable Boolean val) throws GridException {
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
    @Override public GridFuture<?> awaitDeletesAsync() throws GridException {
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
    @Override public boolean evictExclude(GridGgfsPath path, boolean primary) {
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
    @Override public GridGgfsConfiguration configuration() {
        return ggfs.configuration();
    }

    /** {@inheritDoc} */
    @Override public GridGgfsPathSummary summary(GridGgfsPath path) throws GridException {
        return ggfs.summary(path);
    }

    /** {@inheritDoc} */
    @Override public GridGgfsOutputStream create(GridGgfsPath path, boolean overwrite) throws GridException {
        return ggfs.create(path, overwrite);
    }

    /** {@inheritDoc} */
    @Override public GridGgfsOutputStream create(GridGgfsPath path, int bufSize, boolean overwrite, int replication,
        long blockSize, @Nullable Map<String, String> props) throws GridException {
        return ggfs.create(path, bufSize, overwrite, replication, blockSize, props);
    }

    /** {@inheritDoc} */
    @Override public GridGgfsOutputStream create(GridGgfsPath path, int bufSize, boolean overwrite,
        @Nullable IgniteUuid affKey, int replication, long blockSize, @Nullable Map<String, String> props)
        throws GridException {
        return ggfs.create(path, bufSize, overwrite, affKey, replication, blockSize, props);
    }

    /** {@inheritDoc} */
    @Override public GridGgfsOutputStream append(GridGgfsPath path, boolean create) throws GridException {
        return ggfs.append(path, create);
    }

    /** {@inheritDoc} */
    @Override public GridGgfsOutputStream append(GridGgfsPath path, int bufSize, boolean create,
        @Nullable Map<String, String> props) throws GridException {
        return ggfs.append(path, bufSize, create, props);
    }

    /** {@inheritDoc} */
    @Override public void setTimes(GridGgfsPath path, long accessTime, long modificationTime) throws GridException {
        ggfs.setTimes(path, accessTime, modificationTime);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsBlockLocation> affinity(GridGgfsPath path, long start, long len)
        throws GridException {
        return ggfs.affinity(path, start, len);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsBlockLocation> affinity(GridGgfsPath path, long start, long len, long maxLen)
        throws GridException {
        return ggfs.affinity(path, start, len, maxLen);
    }

    /** {@inheritDoc} */
    @Override public GridGgfsMetrics metrics() throws GridException {
        return ggfs.metrics();
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() throws GridException {
        ggfs.resetMetrics();
    }

    /** {@inheritDoc} */
    @Override public long size(GridGgfsPath path) throws GridException {
        return ggfs.size(path);
    }

    /** {@inheritDoc} */
    @Override public boolean exists(GridGgfsPath path) throws GridException {
        return ggfs.exists(path);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridGgfsFile update(GridGgfsPath path, Map<String, String> props) throws GridException {
        return ggfs.update(path, props);
    }

    /** {@inheritDoc} */
    @Override public void rename(GridGgfsPath src, GridGgfsPath dest) throws GridException {
        ggfs.rename(src, dest);
    }

    /** {@inheritDoc} */
    @Override public boolean delete(GridGgfsPath path, boolean recursive) throws GridException {
        return ggfs.delete(path, recursive);
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(GridGgfsPath path) throws GridException {
        ggfs.mkdirs(path);
    }

    /** {@inheritDoc} */
    @Override public void mkdirs(GridGgfsPath path, @Nullable Map<String, String> props) throws GridException {
        ggfs.mkdirs(path, props);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsPath> listPaths(GridGgfsPath path) throws GridException {
        return ggfs.listPaths(path);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridGgfsFile> listFiles(GridGgfsPath path) throws GridException {
        return ggfs.listFiles(path);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridGgfsFile info(GridGgfsPath path) throws GridException {
        return ggfs.info(path);
    }

    /** {@inheritDoc} */
    @Override public long usedSpaceSize() throws GridException {
        return ggfs.usedSpaceSize();
    }

    /** {@inheritDoc} */
    @Override public Map<String, String> properties() {
        return ggfs.properties();
    }
}
