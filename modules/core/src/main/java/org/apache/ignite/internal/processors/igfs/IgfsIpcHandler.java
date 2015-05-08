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

import org.apache.ignite.*;
import org.apache.ignite.igfs.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.igfs.common.*;
import org.apache.ignite.internal.processors.closure.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

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
    private IgfsEx igfs;

    /** Resource ID generator. */
    private AtomicLong rsrcIdGen = new AtomicLong();

    /** Stopping flag. */
    private volatile boolean stopping;

    /**
     * Constructs IGFS IPC handler.
     *
     * @param igfsCtx Context.
     */
    IgfsIpcHandler(IgfsContext igfsCtx) {
        assert igfsCtx != null;

        ctx = igfsCtx.kernalContext();
        igfs = igfsCtx.igfs();

        // Keep buffer size multiple of block size so no extra byte array copies is performed.
        bufSize = igfsCtx.configuration().getBlockSize() * 2;

        log = ctx.log(IgfsIpcHandler.class);
    }

    /** {@inheritDoc} */
    @Override public void stop() throws IgniteCheckedException {
        stopping = true;
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
        final IgfsMessage msg, DataInput in) {
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
                    IgfsMessage res = execute(ses, cmd, msg, in);

                    fut = res == null ? null : new GridFinishedFuture<>(res);

                    break;
                }

                // Execute command asynchronously in user's pool.
                default: {
                    fut = ctx.closure().callLocalSafe(new GridPlainCallable<IgfsMessage>() {
                        @Override public IgfsMessage call() throws Exception {
                            // No need to pass data input for non-write-block commands.
                            return execute(ses, cmd, msg, null);
                        }
                    }, GridClosurePolicy.IGFS_POOL);
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
        @Nullable DataInput in)
        throws Exception {
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
        if (!F.eq(ctx.gridName(), req.gridName()))
            throw new IgniteCheckedException("Failed to perform handshake because actual Grid name differs from expected " +
                "[expected=" + req.gridName() + ", actual=" + ctx.gridName() + ']');

        if (!F.eq(igfs.name(), req.igfsName()))
            throw new IgniteCheckedException("Failed to perform handshake because actual IGFS name differs from expected " +
                "[expected=" + req.igfsName() + ", actual=" + igfs.name() + ']');

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
    private IgfsMessage processPathControlRequest(IgfsClientSession ses, IgfsIpcCommand cmd,
        IgfsMessage msg) throws IgniteCheckedException {
        IgfsPathControlRequest req = (IgfsPathControlRequest)msg;

        if (log.isDebugEnabled())
            log.debug("Processing path control request [igfsName=" + igfs.name() + ", req=" + req + ']');

        IgfsControlResponse res = new IgfsControlResponse();

        try {
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

                    IgfsFileInfo info = new IgfsFileInfo(igfsIn.fileInfo(), null,
                        igfsIn.fileInfo().modificationTime());

                    res.response(new IgfsInputStreamDescriptor(streamId, info.length()));

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
                assert rsrcId != null : "Missing stream ID";

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
