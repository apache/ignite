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

package org.apache.ignite.internal.processors.hadoop.impl.igfs;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.logging.Log;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.Ignition;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathSummary;
import org.apache.ignite.igfs.IgfsUserContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.igfs.IgfsEx;
import org.apache.ignite.internal.processors.igfs.IgfsHandshakeResponse;
import org.apache.ignite.internal.processors.igfs.IgfsStatus;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteOutClosure;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteState.STARTED;

/**
 * Communication with grid in the same process.
 */
public class HadoopIgfsInProc implements HadoopIgfsEx {
    /** Ignite client reference counters (node name, reference count). */
    private static final Map<String, Integer> refCnts = new HashMap<>();

    /** Reference count monitor. */
    private static final Lock refCntsLock = new ReentrantLock();

    /** Target IGFS. */
    protected final IgfsEx igfs;

    /** Buffer size. */
    private final int bufSize;

    /** Event listeners. */
    private final Map<HadoopIgfsStreamDelegate, HadoopIgfsStreamEventListener> lsnrs =
        new ConcurrentHashMap<>();

    /** Logger. */
    private final Log log;

    /** The user this Igfs works on behalf of. */
    private final String user;

    /**
     * Constructor.
     *
     * @param igfs Target IGFS.
     * @param log Log.
     * @param userName User name.
     */
    private HadoopIgfsInProc(IgfsEx igfs, Log log, String userName) {
        this.igfs = igfs;

        this.log = log;

        bufSize = igfs.configuration().getBlockSize() * 2;

        user = IgfsUtils.fixUserName(userName);
    }


    /**
     * Creates instance of the HadoopIgfsInProcWithIgniteRefsCount by IGFS name.
     *
     * @param igfsName Target IGFS name.
     * @param log Log.
     * @param userName User name.
     * @return HadoopIgfsInProcWithIgniteRefsCount instance. {@code null} if the IGFS not fount in the current VM.
     */
    public static HadoopIgfsInProc create(String igfsName, Log log, String userName) {

        for (Ignite ignite : Ignition.allGrids()) {
            HadoopIgfsInProc delegate = create(ignite, igfsName, log, userName);

            if (delegate != null)
                return delegate;
        }

        return null;
    }

    /**
     * Creates instance of the HadoopIgfsInProcWithIgniteRefsCount by IGFS name.
     *
     * @param ignite Ignite instance.
     * @param igfsName Target IGFS name.
     * @param log Log.
     * @param userName User name.
     * @return HadoopIgfsInProcWithIgniteRefsCount instance. {@code null} if the IGFS not found
     *      in the specified ignite instance.
     */
    public static HadoopIgfsInProc create(Ignite ignite, String igfsName, Log log, String userName) {
        assert ignite != null;

        lockRefCount();

        try {
            if (Ignition.state(ignite.name()) == STARTED) {
                try {
                    for (IgniteFileSystem fs : ignite.fileSystems()) {
                        if (F.eq(fs.name(), igfsName)) {
                            Integer cnt = refCnts.get(ignite.name());

                            if (cnt != null)
                                refCnts.put(ignite.name(), cnt + 1);

                            return new HadoopIgfsInProc((IgfsEx)fs, log, userName);
                        }
                    }
                }
                catch (IllegalStateException ignore) {
                    // May happen if the grid state has changed:
                }
            }
        }
        finally {
            unlockRefCount();
        }

        return null;
    }

    /**
     * Register ignite node at the reference counter map.
     * @param nodeName Ignite node name.
     */
    public static void addManagedIgniteInstance(String nodeName) {
        assert !refCnts.containsKey(nodeName) : "The ignite instance already exists in the ref count map";

        refCnts.put(nodeName, 0);
    }

    /** {@inheritDoc} */
    @Override public IgfsHandshakeResponse handshake(final String logDir) {
        return IgfsUserContext.doAs(user, new IgniteOutClosure<IgfsHandshakeResponse>() {
            @Override public IgfsHandshakeResponse apply() {
                igfs.clientLogDirectory(logDir);

                return new IgfsHandshakeResponse(igfs.name(), igfs.proxyPaths(), igfs.groupBlockSize(),
                    igfs.globalSampling());
                }
         });
    }

    /**
     * Call onClose for all listeners.
     */
    private void notifyListenersOnClose() {
        // Perform cleanup.
        for (HadoopIgfsStreamEventListener lsnr : lsnrs.values()) {
            try {
                lsnr.onClose();
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to notify stream event listener", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void close(boolean force) {
        notifyListenersOnClose();

        String gridName = igfs.context().kernalContext().grid().name();

        lockRefCount();

        try {
            Integer cnt = refCnts.get(gridName);

            if (cnt != null) {
                // The node was created by this HadoopIgfsWrapper.
                // The node must be stopped when there are not opened filesystems that are used one.
                if (cnt > 1)
                    refCnts.put(gridName, cnt - 1);
                else {
                    refCnts.remove(gridName);

                    G.stop(gridName, false);
                }
            }
        }
        finally {
            unlockRefCount();
        }
    }

    /** {@inheritDoc} */
    @Override public IgfsFile info(final IgfsPath path) throws IgniteCheckedException {
        try {
            return IgfsUserContext.doAs(user, new IgniteOutClosure<IgfsFile>() {
                @Override public IgfsFile apply() {
                    return igfs.info(path);
                }
            });
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException ignored) {
            throw new HadoopIgfsCommunicationException("Failed to get file info because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public IgfsFile update(final IgfsPath path, final Map<String, String> props) throws
        IgniteCheckedException {
        try {
            return IgfsUserContext.doAs(user, new IgniteOutClosure<IgfsFile>() {
                @Override public IgfsFile apply() {
                    return igfs.update(path, props);
                }
            });
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException ignored) {
            throw new HadoopIgfsCommunicationException("Failed to update file because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public Boolean setTimes(final IgfsPath path, final long accessTime, final long modificationTime)
        throws IgniteCheckedException {
        try {
            IgfsUserContext.doAs(user, new IgniteOutClosure<Void>() {
                @Override public Void apply() {
                    igfs.setTimes(path, accessTime, modificationTime);

                    return null;
                }
            });

            return true;
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException ignored) {
            throw new HadoopIgfsCommunicationException("Failed to set path times because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public Boolean rename(final IgfsPath src, final IgfsPath dest) throws IgniteCheckedException {
        try {
            IgfsUserContext.doAs(user, new IgniteOutClosure<Void>() {
                @Override public Void apply() {
                    igfs.rename(src, dest);

                    return null;
                }
            });

            return true;
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException ignored) {
            throw new HadoopIgfsCommunicationException("Failed to rename path because Grid is stopping: " + src);
        }
    }

    /** {@inheritDoc} */
    @Override public Boolean delete(final IgfsPath path, final boolean recursive) throws IgniteCheckedException {
        try {
            return IgfsUserContext.doAs(user, new IgniteOutClosure<Boolean>() {
                @Override public Boolean apply() {
                    return igfs.delete(path, recursive);
                }
            });
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException ignored) {
            throw new HadoopIgfsCommunicationException("Failed to delete path because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public IgfsStatus fsStatus() throws IgniteCheckedException {
        try {
            return IgfsUserContext.doAs(user, new Callable<IgfsStatus>() {
                @Override public IgfsStatus call() throws IgniteCheckedException {
                    return igfs.globalSpace();
                }
            });
        }
        catch (IllegalStateException ignored) {
            throw new HadoopIgfsCommunicationException("Failed to get file system status because Grid is stopping.");
        }
        catch (IgniteCheckedException | RuntimeException | Error e) {
            throw e;
        }
        catch (Exception ignored) {
            throw new AssertionError("Must never go there.");
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsPath> listPaths(final IgfsPath path) throws IgniteCheckedException {
        try {
            return IgfsUserContext.doAs(user, new IgniteOutClosure<Collection<IgfsPath>>() {
                @Override public Collection<IgfsPath> apply() {
                    return igfs.listPaths(path);
                }
            });
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException ignored) {
            throw new HadoopIgfsCommunicationException("Failed to list paths because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsFile> listFiles(final IgfsPath path) throws IgniteCheckedException {
        try {
            return IgfsUserContext.doAs(user, new IgniteOutClosure<Collection<IgfsFile>>() {
                @Override public Collection<IgfsFile> apply() {
                    return igfs.listFiles(path);
                }
            });
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException ignored) {
            throw new HadoopIgfsCommunicationException("Failed to list files because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public Boolean mkdirs(final IgfsPath path, final Map<String, String> props)
        throws IgniteCheckedException {
        try {
            IgfsUserContext.doAs(user, new IgniteOutClosure<Void>() {
                @Override public Void apply() {
                    igfs.mkdirs(path, props);

                    return null;
                }
            });

            return true;
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException ignored) {
            throw new HadoopIgfsCommunicationException("Failed to create directory because Grid is stopping: " +
                path);
        }
    }

    /** {@inheritDoc} */
    @Override public IgfsPathSummary contentSummary(final IgfsPath path) throws IgniteCheckedException {
        try {
            return IgfsUserContext.doAs(user, new IgniteOutClosure<IgfsPathSummary>() {
                @Override public IgfsPathSummary apply() {
                    return igfs.summary(path);
                }
            });
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException ignored) {
            throw new HadoopIgfsCommunicationException("Failed to get content summary because Grid is stopping: " +
                path);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsBlockLocation> affinity(final IgfsPath path, final long start, final long len)
        throws IgniteCheckedException {
        try {
            return IgfsUserContext.doAs(user, new IgniteOutClosure<Collection<IgfsBlockLocation>>() {
                @Override public Collection<IgfsBlockLocation> apply() {
                    return igfs.affinity(path, start, len);
                }
            });
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException ignored) {
            throw new HadoopIgfsCommunicationException("Failed to get affinity because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public HadoopIgfsStreamDelegate open(final IgfsPath path) throws IgniteCheckedException {
        try {
            return IgfsUserContext.doAs(user, new IgniteOutClosure<HadoopIgfsStreamDelegate>() {
                @Override public HadoopIgfsStreamDelegate apply() {
                    IgfsInputStream stream = igfs.open(path, bufSize);

                    return new HadoopIgfsStreamDelegate(HadoopIgfsInProc.this, stream, stream.length());
                }
            });
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException ignored) {
            throw new HadoopIgfsCommunicationException("Failed to open file because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public HadoopIgfsStreamDelegate open(final IgfsPath path, final int seqReadsBeforePrefetch)
        throws IgniteCheckedException {
        try {
            return IgfsUserContext.doAs(user, new IgniteOutClosure<HadoopIgfsStreamDelegate>() {
                @Override public HadoopIgfsStreamDelegate apply() {
                    IgfsInputStream stream = igfs.open(path, bufSize, seqReadsBeforePrefetch);

                    return new HadoopIgfsStreamDelegate(HadoopIgfsInProc.this, stream, stream.length());
                }
            });
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException ignored) {
            throw new HadoopIgfsCommunicationException("Failed to open file because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public HadoopIgfsStreamDelegate create(final IgfsPath path, final boolean overwrite,
        final boolean colocate, final int replication, final long blockSize, final @Nullable Map<String, String> props)
        throws IgniteCheckedException {
        try {
            return IgfsUserContext.doAs(user, new IgniteOutClosure<HadoopIgfsStreamDelegate>() {
                @Override public HadoopIgfsStreamDelegate apply() {
                    IgfsOutputStream stream = igfs.create(path, bufSize, overwrite,
                        colocate ? igfs.nextAffinityKey() : null, replication, blockSize, props);

                    return new HadoopIgfsStreamDelegate(HadoopIgfsInProc.this, stream);
                }
            });
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException ignored) {
            throw new HadoopIgfsCommunicationException("Failed to create file because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public HadoopIgfsStreamDelegate append(final IgfsPath path, final boolean create,
        final @Nullable Map<String, String> props) throws IgniteCheckedException {
        try {
            return IgfsUserContext.doAs(user, new IgniteOutClosure<HadoopIgfsStreamDelegate>() {
                @Override public HadoopIgfsStreamDelegate apply() {
                    IgfsOutputStream stream = igfs.append(path, bufSize, create, props);

                    return new HadoopIgfsStreamDelegate(HadoopIgfsInProc.this, stream);
                }
            });
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException ignored) {
            throw new HadoopIgfsCommunicationException("Failed to append file because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<byte[]> readData(HadoopIgfsStreamDelegate delegate, long pos, int len,
        @Nullable byte[] outBuf, int outOff, int outLen) {
        IgfsInputStream stream = delegate.target();

        try {
            byte[] res = null;

            if (outBuf != null) {
                int outTailLen = outBuf.length - outOff;

                if (len <= outTailLen)
                    stream.readFully(pos, outBuf, outOff, len);
                else {
                    stream.readFully(pos, outBuf, outOff, outTailLen);

                    int remainderLen = len - outTailLen;

                    res = new byte[remainderLen];

                    stream.readFully(pos, res, 0, remainderLen);
                }
            } else {
                res = new byte[len];

                stream.readFully(pos, res, 0, len);
            }

            return new GridFinishedFuture<>(res);
        }
        catch (IllegalStateException | IOException e) {
            HadoopIgfsStreamEventListener lsnr = lsnrs.get(delegate);

            if (lsnr != null)
                lsnr.onError(e.getMessage());

            return new GridFinishedFuture<>(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeData(HadoopIgfsStreamDelegate delegate, byte[] data, int off, int len)
        throws IOException {
        try {
            IgfsOutputStream stream = delegate.target();

            stream.write(data, off, len);
        }
        catch (IllegalStateException | IOException e) {
            HadoopIgfsStreamEventListener lsnr = lsnrs.get(delegate);

            if (lsnr != null)
                lsnr.onError(e.getMessage());

            if (e instanceof IllegalStateException)
                throw new IOException("Failed to write data to IGFS stream because Grid is stopping.", e);
            else
                throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void flush(HadoopIgfsStreamDelegate delegate) throws IOException {
        try {
            IgfsOutputStream stream = delegate.target();

            stream.flush();
        }
        catch (IllegalStateException | IOException e) {
            HadoopIgfsStreamEventListener lsnr = lsnrs.get(delegate);

            if (lsnr != null)
                lsnr.onError(e.getMessage());

            if (e instanceof IllegalStateException)
                throw new IOException("Failed to flush data to IGFS stream because Grid is stopping.", e);
            else
                throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void closeStream(HadoopIgfsStreamDelegate desc) throws IOException {
        Closeable closeable = desc.target();

        try {
            closeable.close();
        }
        catch (IllegalStateException e) {
            throw new IOException("Failed to close IGFS stream because Grid is stopping.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void addEventListener(HadoopIgfsStreamDelegate delegate,
        HadoopIgfsStreamEventListener lsnr) {
        HadoopIgfsStreamEventListener lsnr0 = lsnrs.put(delegate, lsnr);

        assert lsnr0 == null || lsnr0 == lsnr;

        if (log.isDebugEnabled())
            log.debug("Added stream event listener [delegate=" + delegate + ']');
    }

    /** {@inheritDoc} */
    @Override public void removeEventListener(HadoopIgfsStreamDelegate delegate) {
        HadoopIgfsStreamEventListener lsnr0 = lsnrs.remove(delegate);

        if (lsnr0 != null && log.isDebugEnabled())
            log.debug("Removed stream event listener [delegate=" + delegate + ']');
    }

    /** {@inheritDoc} */
    @Override public String user() {
        return user;
    }

    /** Lock reference count. */
    public static void lockRefCount() {
        refCntsLock.lock();
    }

    /** Unlock reference count. */
    public static void unlockRefCount() {
        refCntsLock.unlock();
    }
}
