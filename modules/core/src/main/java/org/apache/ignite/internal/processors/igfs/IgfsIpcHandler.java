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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsOutOfSpaceException;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsUserContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.igfs.common.IgfsControlResponse;
import org.apache.ignite.internal.igfs.common.IgfsHandshakeRequest;
import org.apache.ignite.internal.igfs.common.IgfsIpcCommand;
import org.apache.ignite.internal.igfs.common.IgfsMessage;
import org.apache.ignite.internal.igfs.common.IgfsPathControlRequest;
import org.apache.ignite.internal.igfs.common.IgfsStreamControlRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * IGFS IPC handler.
 */
class IgfsIpcHandler implements IgfsServerHandler {
    /** For test purposes only. */
    @SuppressWarnings("UnusedDeclaration")
    private static boolean errWrite;

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Log. */
    private IgniteLogger log;

    /** Buffer size. */
    private final int bufSize; // Buffer size. Must not be less then file block size.

    /** IGFS instance for this handler. */
    private final IgfsEx igfs;

    /** Resource ID generator. */
    private final AtomicLong rsrcIdGen = new AtomicLong();

    /** Thread pool. */
    private volatile IgniteThreadPoolExecutor pool;

    /** Stopping flag. */
    private volatile boolean stopping;

    /**
     * Constructs IGFS IPC handler.
     *
     * @param igfsCtx Context.
     * @param endpointCfg Endpoint configuration.
     * @param mgmt Management flag.
     */
    IgfsIpcHandler(IgfsContext igfsCtx, IgfsIpcEndpointConfiguration endpointCfg, boolean mgmt) {
        assert igfsCtx != null;

        ctx = igfsCtx.kernalContext();
        igfs = igfsCtx.igfs();

        // Keep buffer size multiple of block size so no extra byte array copies is performed.
        bufSize = igfsCtx.configuration().getBlockSize() * 2;

        // Create thread pool for request handling.
        int threadCnt = endpointCfg.getThreadCount();

        String prefix = "igfs-" + igfsCtx.igfs().name() + (mgmt ? "mgmt-" : "") + "-ipc";

        pool = new IgniteThreadPoolExecutor(prefix, igfsCtx.kernalContext().gridName(), threadCnt, threadCnt,
            Long.MAX_VALUE, new LinkedBlockingQueue<Runnable>());

        log = ctx.log(IgfsIpcHandler.class);
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteCheckedException {
        stopping = true;

        U.shutdownNow(getClass(), pool, log);

        pool = null;
    }

    /** {@inheritDoc} */
    @Override public void onClosed(IgfsClientSession ses) {
        Iterator<Closeable> it = ses.registeredResources();

        while (it.hasNext()) {
            Closeable stream = it.next();

            try {
                stream.close();
            }
            catch (IOException e) {
                U.warn(log, "Failed to close opened stream on client close event (will continue) [ses=" + ses +
                    ", stream=" + stream + ']', e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<IgfsMessage> handleAsync(final IgfsClientSession ses,
        final IgfsMessage msg, final DataInput in) {
        try {
            // Even if will be closed right after this call, response write error will be ignored.
            if (stopping)
                return null;

            final IgfsIpcCommand cmd = msg.command();

            IgniteInternalFuture<IgfsMessage> fut;

            switch (cmd) {
                // Execute not-blocking command synchronously in worker thread.
                case WRITE_BLOCK:
                case MAKE_DIRECTORIES:
                case LIST_FILES:
                case LIST_PATHS: {
                    fut = executeSynchronously(ses, cmd, msg, in);

                    break;
                }

                // Execute command asynchronously in pool.
                default: {
                    try {
                        final GridFutureAdapter<IgfsMessage> fut0 = new GridFutureAdapter<>();

                        pool.execute(new Runnable() {
                            @Override public void run()  {
                                try {
                                    fut0.onDone(execute(ses, cmd, msg, in));
                                }
                                catch (Exception e) {
                                    fut0.onDone(e);
                                }
                            }
                        });

                        fut = fut0;
                    }
                    catch (RejectedExecutionException ignored) {
                        fut = executeSynchronously(ses, cmd, msg, in);
                    }
                }
            }

            // Pack result object into response format.
            return fut;
        }
        catch (Exception e) {
            return new GridFinishedFuture<>(e);
        }
    }

    /**
     * Execute operation synchronously.
     *
     * @param ses Session.
     * @param cmd Command.
     * @param msg Message.
     * @param in Input.
     * @return Result.
     * @throws Exception If failed.
     */
    @Nullable private IgniteInternalFuture<IgfsMessage> executeSynchronously(IgfsClientSession ses,
        IgfsIpcCommand cmd, IgfsMessage msg, DataInput in) throws Exception {
        IgfsMessage res = execute(ses, cmd, msg, in);

        return res == null ? null : new GridFinishedFuture<>(res);
    }

    /**
     * Execute IGFS command.
     *
     * @param ses Client connection session.
     * @param cmd Command to execute.
     * @param msg Message to process.
     * @param in Data input in case of block write command.
     * @return Command execution result.
     * @throws Exception If failed.
     */
    private IgfsMessage execute(IgfsClientSession ses, IgfsIpcCommand cmd, IgfsMessage msg,
        @Nullable DataInput in) throws Exception {
        switch (cmd) {
            case HANDSHAKE:
                return processHandshakeRequest((IgfsHandshakeRequest)msg);

            case STATUS:
                return processStatusRequest();

            case EXISTS:
            case INFO:
            case PATH_SUMMARY:
            case UPDATE:
            case RENAME:
            case DELETE:
            case MAKE_DIRECTORIES:
            case LIST_PATHS:
            case LIST_FILES:
            case SET_TIMES:
            case AFFINITY:
            case OPEN_READ:
            case OPEN_CREATE:
            case OPEN_APPEND:
                return processPathControlRequest(ses, cmd, msg);

            case CLOSE:
            case READ_BLOCK:
            case WRITE_BLOCK:
                return processStreamControlRequest(ses, cmd, msg, in);

            default:
                throw new IgniteCheckedException("Unsupported IPC command: " + cmd);
        }
    }

    /**
     * Processes handshake request.
     *
     * @param req Handshake request.
     * @return Response message.
     * @throws IgniteCheckedException In case of handshake failure.
     */
    private IgfsMessage processHandshakeRequest(IgfsHandshakeRequest req) throws IgniteCheckedException {
        if (req.gridName() != null && !F.eq(ctx.gridName(), req.gridName()))
            throw new IgniteCheckedException("Failed to perform handshake because existing Grid name " +
                "differs from requested [requested=" + req.gridName() + ", existing=" + ctx.gridName() + ']');

        if (req.igfsName() != null && !F.eq(igfs.name(), req.igfsName()))
            throw new IgniteCheckedException("Failed to perform handshake because existing IGFS name " +
                "differs from requested [requested=" + req.igfsName() + ", existing=" + igfs.name() + ']');

        IgfsControlResponse res = new IgfsControlResponse();

        igfs.clientLogDirectory(req.logDirectory());

        IgfsHandshakeResponse handshake = new IgfsHandshakeResponse(igfs.name(), igfs.proxyPaths(),
            igfs.groupBlockSize(), igfs.globalSampling());

        res.handshake(handshake);

        return res;
    }

    /**
     * Processes status request.
     *
     * @return Status response.
     * @throws IgniteCheckedException If failed.
     */
    private IgfsMessage processStatusRequest() throws IgniteCheckedException {
        IgfsStatus status = igfs.globalSpace();

        IgfsControlResponse res = new IgfsControlResponse();

        res.status(status);

        return res;
    }

    /**
     * Processes path control request.
     *
     * @param ses Session.
     * @param cmd Command.
     * @param msg Message.
     * @return Response message.
     * @throws IgniteCheckedException If failed.
     */
    private IgfsMessage processPathControlRequest(final IgfsClientSession ses, final IgfsIpcCommand cmd,
        IgfsMessage msg) throws IgniteCheckedException {
        final IgfsPathControlRequest req = (IgfsPathControlRequest)msg;

        if (log.isDebugEnabled())
            log.debug("Processing path control request [igfsName=" + igfs.name() + ", req=" + req + ']');

        final IgfsControlResponse res = new IgfsControlResponse();

        final String userName = req.userName();

        assert userName != null;

        try {
            IgfsUserContext.doAs(userName, new IgniteOutClosure<Object>() {
                @Override public Void apply() {
                    switch (cmd) {
                        case EXISTS:
                            res.response(igfs.exists(req.path()));

                            break;

                        case INFO:
                            res.response(igfs.info(req.path()));

                            break;

                        case PATH_SUMMARY:
                            res.response(igfs.summary(req.path()));

                            break;

                        case UPDATE:
                            res.response(igfs.update(req.path(), req.properties()));

                            break;

                        case RENAME:
                            igfs.rename(req.path(), req.destinationPath());

                            res.response(true);

                            break;

                        case DELETE:
                            res.response(igfs.delete(req.path(), req.flag()));

                            break;

                        case MAKE_DIRECTORIES:
                            igfs.mkdirs(req.path(), req.properties());

                            res.response(true);

                            break;

                        case LIST_PATHS:
                            res.paths(igfs.listPaths(req.path()));

                            break;

                        case LIST_FILES:
                            res.files(igfs.listFiles(req.path()));

                            break;

                        case SET_TIMES:
                            igfs.setTimes(req.path(), req.accessTime(), req.modificationTime());

                            res.response(true);

                            break;

                        case AFFINITY:
                            res.locations(igfs.affinity(req.path(), req.start(), req.length()));

                            break;

                        case OPEN_READ: {
                            IgfsInputStreamAdapter igfsIn = !req.flag() ? igfs.open(req.path(), bufSize) :
                                igfs.open(req.path(), bufSize, req.sequentialReadsBeforePrefetch());

                            long streamId = registerResource(ses, igfsIn);

                            if (log.isDebugEnabled())
                                log.debug("Opened IGFS input stream for file read [igfsName=" + igfs.name() + ", path=" +
                                    req.path() + ", streamId=" + streamId + ", ses=" + ses + ']');

                            res.response(new IgfsInputStreamDescriptor(streamId, igfsIn.fileInfo().length()));

                            break;
                        }

                        case OPEN_CREATE: {
                            long streamId = registerResource(ses, igfs.create(
                                req.path(),       // Path.
                                bufSize,          // Buffer size.
                                req.flag(),       // Overwrite if exists.
                                affinityKey(req), // Affinity key based on replication factor.
                                req.replication(),// Replication factor.
                                req.blockSize(),  // Block size.
                                req.properties()  // File properties.
                            ));

                            if (log.isDebugEnabled())
                                log.debug("Opened IGFS output stream for file create [igfsName=" + igfs.name() + ", path=" +
                                    req.path() + ", streamId=" + streamId + ", ses=" + ses + ']');

                            res.response(streamId);

                            break;
                        }

                        case OPEN_APPEND: {
                            long streamId = registerResource(ses, igfs.append(
                                req.path(),        // Path.
                                bufSize,           // Buffer size.
                                req.flag(),        // Create if absent.
                                req.properties()   // File properties.
                            ));

                            if (log.isDebugEnabled())
                                log.debug("Opened IGFS output stream for file append [igfsName=" + igfs.name() + ", path=" +
                                    req.path() + ", streamId=" + streamId + ", ses=" + ses + ']');

                            res.response(streamId);

                            break;
                        }

                        default:
                            assert false : "Unhandled path control request command: " + cmd;

                            break;
                    }

                    return null;
                }
            });
        }
        catch (IgniteException e) {
            throw new IgniteCheckedException(e);
        }

        if (log.isDebugEnabled())
            log.debug("Finished processing path control request [igfsName=" + igfs.name() + ", req=" + req +
                ", res=" + res + ']');

        return res;
    }

    /**
     * Processes stream control request.
     *
     * @param ses Session.
     * @param cmd Command.
     * @param msg Message.
     * @param in Data input to read.
     * @return Response message if needed.
     * @throws IgniteCheckedException If failed.
     * @throws IOException If failed.
     */
    private IgfsMessage processStreamControlRequest(IgfsClientSession ses, IgfsIpcCommand cmd,
        IgfsMessage msg, DataInput in) throws IgniteCheckedException, IOException {
        IgfsStreamControlRequest req = (IgfsStreamControlRequest)msg;

        Long rsrcId = req.streamId();

        IgfsControlResponse resp = new IgfsControlResponse();

        switch (cmd) {
            case CLOSE: {
                Closeable res = resource(ses, rsrcId);

                if (log.isDebugEnabled())
                    log.debug("Requested to close resource [igfsName=" + igfs.name() + ", rsrcId=" + rsrcId +
                        ", res=" + res + ']');

                if (res == null)
                    throw new IgniteCheckedException("Resource to close not found: " + rsrcId);

                try {
                    res.close();
                }
                catch (IOException e) {
                    // Unwrap OutOfSpaceException, if has one.
                    IgfsOutOfSpaceException space = X.cause(e, IgfsOutOfSpaceException.class);

                    if (space != null)
                        throw space;

                    throw e;
                }

                boolean success = ses.unregisterResource(rsrcId, res);

                assert success : "Failed to unregister resource [igfsName=" + igfs.name() + ", rsrcId=" + rsrcId +
                    ", res=" + res + ']';

                if (log.isDebugEnabled())
                    log.debug("Closed IGFS stream [igfsName=" + igfs.name() + ", streamId=" + rsrcId +
                        ", ses=" + ses + ']');

                resp.response(true);

                break;
            }

            case READ_BLOCK: {
                long pos = req.position();
                int size = req.length();

                IgfsInputStreamAdapter igfsIn = (IgfsInputStreamAdapter)resource(ses, rsrcId);

                if (igfsIn == null)
                    throw new IgniteCheckedException("Input stream not found (already closed?): " + rsrcId);

                byte[][] chunks = igfsIn.readChunks(pos, size);

                resp.response(chunks);

                // Calculate number of read bytes.
                // len = len(first) + (n - 2) * len(block) + len(last).
                int len = 0;

                if (chunks.length > 0)
                    len += chunks[0].length;

                if (chunks.length > 1)
                    len += chunks[chunks.length - 1].length;

                if (chunks.length > 2)
                    len += chunks[1].length * (chunks.length - 2);

                resp.length(len);

                break;
            }

            case WRITE_BLOCK: {
                IgfsOutputStream out = (IgfsOutputStream)resource(ses, rsrcId);

                if (out == null)
                    throw new IgniteCheckedException("Output stream not found (already closed?): " + rsrcId);

                int writeLen = req.length();

                try {
                    out.transferFrom(in, writeLen);

                    if (errWrite)
                        throw new IOException("Failed to write data to server (test).");

                    // No response needed.
                    return null;
                }
                catch (IOException e) {
                    resp.error(rsrcId, e.getMessage());

                    break;
                }
            }

            default:
                assert false;

                break;
        }

        return resp;
    }

    /**
     * @param req Path control request.
     * @return Affinity key that maps on local node by the time this method is called if replication factor
     *      is {@code 0}, {@code null} otherwise.
     */
    @Nullable private IgniteUuid affinityKey(IgfsPathControlRequest req) {
        // Do not generate affinity key for replicated or near-only cache.
        if (!req.colocate()) {
            if (log.isDebugEnabled())
                log.debug("Will not generate affinity key for path control request [igfsName=" + igfs.name() +
                    ", req=" + req + ']');

            return null;
        }

        IgniteUuid key = igfs.nextAffinityKey();

        if (log.isDebugEnabled())
            log.debug("Generated affinity key for path control request [igfsName=" + igfs.name() +
                ", req=" + req + ", key=" + key + ']');

        return key;
    }

    /**
     * Registers closeable resource within client session.
     *
     * @param ses IPC session.
     * @param rsrc Resource to register.
     * @return Registration resource ID.
     */
    private long registerResource(IgfsClientSession ses, Closeable rsrc) {
        long rsrcId = rsrcIdGen.getAndIncrement();

        boolean registered = ses.registerResource(rsrcId, rsrc);

        assert registered : "Failed to register resource (duplicate id?): " + rsrcId;

        return rsrcId;
    }

    /**
     * Gets resource by resource ID from client session.
     *
     * @param ses Session to get resource from.
     * @param rsrcId Resource ID.
     * @return Registered resource or {@code null} if not found.
     */
    @Nullable private Closeable resource(IgfsClientSession ses, Long rsrcId) {
        return ses.resource(rsrcId);
    }
}