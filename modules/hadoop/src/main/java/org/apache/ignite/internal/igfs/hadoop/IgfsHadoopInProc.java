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

package org.apache.ignite.internal.igfs.hadoop;

import org.apache.commons.logging.*;
import org.apache.ignite.*;
import org.apache.ignite.igfs.*;
import org.apache.ignite.internal.processors.igfs.*;
import org.apache.ignite.internal.util.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Communication with grid in the same process.
 */
public class IgfsHadoopInProc implements IgfsHadoopEx {
    /** Target IGFS. */
    private final IgfsEx igfs;

    /** Buffer size. */
    private final int bufSize;

    /** Event listeners. */
    private final Map<IgfsHadoopStreamDelegate, IgfsHadoopStreamEventListener> lsnrs =
        new ConcurrentHashMap<>();

    /** Logger. */
    private final Log log;

    /**
     * Constructor.
     *
     * @param igfs Target IGFS.
     * @param log Log.
     */
    public IgfsHadoopInProc(IgfsEx igfs, Log log) {
        this.igfs = igfs;
        this.log = log;

        bufSize = igfs.configuration().getBlockSize() * 2;
    }

    /** {@inheritDoc} */
    @Override public IgfsHandshakeResponse handshake(String logDir) {
        igfs.clientLogDirectory(logDir);

        return new IgfsHandshakeResponse(igfs.name(), igfs.proxyPaths(), igfs.groupBlockSize(),
            igfs.globalSampling());
    }

    /** {@inheritDoc} */
    @Override public void close(boolean force) {
        // Perform cleanup.
        for (IgfsHadoopStreamEventListener lsnr : lsnrs.values()) {
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
    @Override public IgfsFile info(IgfsPath path) throws IgniteCheckedException {
        try {
            return igfs.info(path);
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException e) {
            throw new IgfsHadoopCommunicationException("Failed to get file info because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public IgfsFile update(IgfsPath path, Map<String, String> props) throws IgniteCheckedException {
        try {
            return igfs.update(path, props);
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException e) {
            throw new IgfsHadoopCommunicationException("Failed to update file because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public Boolean setTimes(IgfsPath path, long accessTime, long modificationTime) throws IgniteCheckedException {
        try {
            igfs.setTimes(path, accessTime, modificationTime);

            return true;
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException e) {
            throw new IgfsHadoopCommunicationException("Failed to set path times because Grid is stopping: " +
                path);
        }
    }

    /** {@inheritDoc} */
    @Override public Boolean rename(IgfsPath src, IgfsPath dest) throws IgniteCheckedException {
        try {
            igfs.rename(src, dest);

            return true;
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException e) {
            throw new IgfsHadoopCommunicationException("Failed to rename path because Grid is stopping: " + src);
        }
    }

    /** {@inheritDoc} */
    @Override public Boolean delete(IgfsPath path, boolean recursive) throws IgniteCheckedException {
        try {
            return igfs.delete(path, recursive);
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException e) {
            throw new IgfsHadoopCommunicationException("Failed to delete path because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public IgfsStatus fsStatus() throws IgniteCheckedException {
        try {
            return igfs.globalSpace();
        }
        catch (IllegalStateException e) {
            throw new IgfsHadoopCommunicationException("Failed to get file system status because Grid is " +
                "stopping.");
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsPath> listPaths(IgfsPath path) throws IgniteCheckedException {
        try {
            return igfs.listPaths(path);
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException e) {
            throw new IgfsHadoopCommunicationException("Failed to list paths because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsFile> listFiles(IgfsPath path) throws IgniteCheckedException {
        try {
            return igfs.listFiles(path);
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException e) {
            throw new IgfsHadoopCommunicationException("Failed to list files because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public Boolean mkdirs(IgfsPath path, Map<String, String> props) throws IgniteCheckedException {
        try {
            igfs.mkdirs(path, props);

            return true;
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException e) {
            throw new IgfsHadoopCommunicationException("Failed to create directory because Grid is stopping: " +
                path);
        }
    }

    /** {@inheritDoc} */
    @Override public IgfsPathSummary contentSummary(IgfsPath path) throws IgniteCheckedException {
        try {
            return igfs.summary(path);
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException e) {
            throw new IgfsHadoopCommunicationException("Failed to get content summary because Grid is stopping: " +
                path);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<IgfsBlockLocation> affinity(IgfsPath path, long start, long len)
        throws IgniteCheckedException {
        try {
            return igfs.affinity(path, start, len);
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException e) {
            throw new IgfsHadoopCommunicationException("Failed to get affinity because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public IgfsHadoopStreamDelegate open(IgfsPath path) throws IgniteCheckedException {
        try {
            IgfsInputStreamAdapter stream = igfs.open(path, bufSize);

            return new IgfsHadoopStreamDelegate(this, stream, stream.fileInfo().length());
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException e) {
            throw new IgfsHadoopCommunicationException("Failed to open file because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public IgfsHadoopStreamDelegate open(IgfsPath path, int seqReadsBeforePrefetch)
        throws IgniteCheckedException {
        try {
            IgfsInputStreamAdapter stream = igfs.open(path, bufSize, seqReadsBeforePrefetch);

            return new IgfsHadoopStreamDelegate(this, stream, stream.fileInfo().length());
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException e) {
            throw new IgfsHadoopCommunicationException("Failed to open file because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public IgfsHadoopStreamDelegate create(IgfsPath path, boolean overwrite, boolean colocate,
        int replication, long blockSize, @Nullable Map<String, String> props) throws IgniteCheckedException {
        try {
            IgfsOutputStream stream = igfs.create(path, bufSize, overwrite,
                colocate ? igfs.nextAffinityKey() : null, replication, blockSize, props);

            return new IgfsHadoopStreamDelegate(this, stream);
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException e) {
            throw new IgfsHadoopCommunicationException("Failed to create file because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public IgfsHadoopStreamDelegate append(IgfsPath path, boolean create,
        @Nullable Map<String, String> props) throws IgniteCheckedException {
        try {
            IgfsOutputStream stream = igfs.append(path, bufSize, create, props);

            return new IgfsHadoopStreamDelegate(this, stream);
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }
        catch (IllegalStateException e) {
            throw new IgfsHadoopCommunicationException("Failed to append file because Grid is stopping: " + path);
        }
    }

    /** {@inheritDoc} */
    @Override public GridPlainFuture<byte[]> readData(IgfsHadoopStreamDelegate delegate, long pos, int len,
        @Nullable byte[] outBuf, int outOff, int outLen) {
        IgfsInputStreamAdapter stream = delegate.target();

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

            return new GridPlainFutureAdapter<>(res);
        }
        catch (IllegalStateException | IOException e) {
            IgfsHadoopStreamEventListener lsnr = lsnrs.get(delegate);

            if (lsnr != null)
                lsnr.onError(e.getMessage());

            return new GridPlainFutureAdapter<>(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeData(IgfsHadoopStreamDelegate delegate, byte[] data, int off, int len)
        throws IOException {
        try {
            IgfsOutputStream stream = delegate.target();

            stream.write(data, off, len);
        }
        catch (IllegalStateException | IOException e) {
            IgfsHadoopStreamEventListener lsnr = lsnrs.get(delegate);

            if (lsnr != null)
                lsnr.onError(e.getMessage());

            if (e instanceof IllegalStateException)
                throw new IOException("Failed to write data to IGFS stream because Grid is stopping.", e);
            else
                throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void flush(IgfsHadoopStreamDelegate delegate) throws IOException {
        try {
            IgfsOutputStream stream = delegate.target();

            stream.flush();
        }
        catch (IllegalStateException | IOException e) {
            IgfsHadoopStreamEventListener lsnr = lsnrs.get(delegate);

            if (lsnr != null)
                lsnr.onError(e.getMessage());

            if (e instanceof IllegalStateException)
                throw new IOException("Failed to flush data to IGFS stream because Grid is stopping.", e);
            else
                throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void closeStream(IgfsHadoopStreamDelegate desc) throws IOException {
        Closeable closeable = desc.target();

        try {
            closeable.close();
        }
        catch (IllegalStateException e) {
            throw new IOException("Failed to close IGFS stream because Grid is stopping.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void addEventListener(IgfsHadoopStreamDelegate delegate,
        IgfsHadoopStreamEventListener lsnr) {
        IgfsHadoopStreamEventListener lsnr0 = lsnrs.put(delegate, lsnr);

        assert lsnr0 == null || lsnr0 == lsnr;

        if (log.isDebugEnabled())
            log.debug("Added stream event listener [delegate=" + delegate + ']');
    }

    /** {@inheritDoc} */
    @Override public void removeEventListener(IgfsHadoopStreamDelegate delegate) {
        IgfsHadoopStreamEventListener lsnr0 = lsnrs.remove(delegate);

        if (lsnr0 != null && log.isDebugEnabled())
            log.debug("Removed stream event listener [delegate=" + delegate + ']');
    }
}
