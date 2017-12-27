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

package org.apache.ignite.internal.util.nio.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.GridNioException;
import org.apache.ignite.internal.util.nio.GridNioFilterAdapter;
import org.apache.ignite.internal.util.nio.GridNioFinishedFuture;
import org.apache.ignite.internal.util.nio.GridNioFuture;
import org.apache.ignite.internal.util.nio.GridNioSession;
import org.apache.ignite.internal.util.nio.ssl.GridNioSslFilter;
import org.apache.ignite.lang.IgniteInClosure;

import static org.apache.ignite.internal.util.nio.GridNioSessionMetaKey.COMPRESS_META;

/** */
public class GridNioCompressionFilter extends GridNioFilterAdapter {
    /** Logger to use. */
    private IgniteLogger log;

    /** Order. */
    private ByteOrder order;

    /** Allocate direct buffer or heap buffer. */
    private boolean directBuf;

    /** Whether direct mode is used. */
    private boolean directMode;

    /**
     * Creates compress filter.
     *
     * @param directBuf Direct buffer flag.
     * @param order Byte order.
     * @param log Logger to use.
     */
    public GridNioCompressionFilter(boolean directBuf, ByteOrder order, IgniteLogger log) {
        super("Compress filter");

        this.log = log;
        this.directBuf = directBuf;
        this.order = order;
    }

    /**
     *
     * @param directMode Flag indicating whether direct mode is used.
     */
    public void directMode(boolean directMode) {
        this.directMode = directMode;
    }

    /**
     * @return Flag indicating whether direct mode is used.
     */
    public boolean directMode() {
        return directMode;
    }

    /** {@inheritDoc} */
    @Override public void onSessionOpened(GridNioSession ses) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Remote client connected, creating compress handler: " + ses);

        CompressionEngine engine;

        GridCompressionMeta compressMeta = ses.meta(COMPRESS_META.ordinal());

        if (compressMeta == null) {
            engine = new LZ4CompressionEngine();

            compressMeta = new GridCompressionMeta();

            ses.addMeta(COMPRESS_META.ordinal(), compressMeta);
        }
        else {
            engine = compressMeta.compressEngine();

            assert engine != null;
        }

        GridNioCompressionHandler hnd = new GridNioCompressionHandler(this,
            ses,
            engine,
            directBuf,
            order,
            log,
            compressMeta.encodedBuffer());

        compressMeta.handler(hnd);

        ByteBuffer alreadyDecoded = compressMeta.decodedBuffer();

        proceedSessionOpened(ses);

        if (alreadyDecoded != null)
            proceedMessageReceived(ses, alreadyDecoded);
    }

    /** {@inheritDoc} */
    @Override public void onSessionClosed(GridNioSession ses) throws IgniteCheckedException {
        GridNioCompressionHandler hnd = compressionHandler(ses);

        try {
            hnd.shutdown();
        }
        finally {
            proceedSessionClosed(ses);
        }
    }

    /** {@inheritDoc} */
    @Override public void onExceptionCaught(GridNioSession ses, IgniteCheckedException ex)
        throws IgniteCheckedException {
        proceedExceptionCaught(ses, ex);
    }

    /**
     * @param ses Session.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    public void lock(GridNioSession ses) {
        GridNioCompressionHandler hnd = compressionHandler(ses);

        hnd.lock();
    }

    /**
     * @param ses NIO session.
     */
    public void unlock(GridNioSession ses) {
        compressionHandler(ses).unlock();
    }

    /**
     * @param ses Session.
     * @param input Data to compress.
     * @return Output buffer with compressed data.
     * @throws IOException If failed to compress.
     */
    public ByteBuffer compress(GridNioSession ses, ByteBuffer input) throws IOException {
        GridNioCompressionHandler hnd = compressionHandler(ses);

        hnd.lock();

        try {
            return hnd.compress(input);
        }
        finally {
            hnd.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> onSessionWrite(
        GridNioSession ses,
        Object msg,
        boolean fut,
        IgniteInClosure<IgniteException> ackC
    ) throws IgniteCheckedException {
        if (directMode)
            return proceedSessionWrite(ses, msg, fut, ackC);

        ByteBuffer input = checkMessage(ses, msg);

        if (!input.hasRemaining())
            return new GridNioFinishedFuture<Object>(null);

        GridNioCompressionHandler hnd = compressionHandler(ses);

        hnd.lock();

        try {
            hnd.compress(input);

            return hnd.writeNetBuffer(ackC);
        }
        catch (IOException e) {
            throw new GridNioException("Failed to compress data: " + ses, e);
        }
        finally {
            hnd.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
        ByteBuffer input = checkMessage(ses, msg);

        GridNioCompressionHandler hnd = compressionHandler(ses);

        hnd.lock();

        try {
            hnd.messageReceived(input);

            ByteBuffer appBuf = hnd.getApplicationBuffer();

            appBuf.flip();

            if (appBuf.hasRemaining())
                proceedMessageReceived(ses, appBuf);

            appBuf.compact();
        }
        catch (IOException e) {
            throw new GridNioException("Failed to decompress data: " + ses, e);
        }
        finally {
            hnd.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws IgniteCheckedException {
        GridNioSslFilter sslFilter = null;

        if (nextFilter() instanceof GridNioSslFilter) {
            sslFilter = (GridNioSslFilter)nextFilter();

            sslFilter.lock(ses);
        }

        GridNioCompressionHandler hnd = compressionHandler(ses);

        hnd.lock();

        try {
            return shutdownSession(ses, hnd);
        }
        finally {
            hnd.unlock();

            if (sslFilter != null)
                sslFilter.unlock(ses);
        }
    }

    /**
     * Closes underlying TCP connection.
     *
     * @param ses Session to shutdown.
     * @param hnd Compress handler.
     * @throws GridNioException If failed to forward requests to filter chain.
     * @return Close future.
     */
    private GridNioFuture<Boolean> shutdownSession(GridNioSession ses, GridNioCompressionHandler hnd)
        throws IgniteCheckedException {
        hnd.writeNetBuffer(null);

        return proceedSessionClose(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionIdleTimeout(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionWriteTimeout(ses);
    }

    /**
     * Gets compression handler from the session.
     *
     * @param ses Session instance.
     * @return compression handler.
     */
    private GridNioCompressionHandler compressionHandler(GridNioSession ses) {
        GridCompressionMeta compressMeta = ses.meta(COMPRESS_META.ordinal());

        assert compressMeta != null;

        GridNioCompressionHandler hnd = compressMeta.handler();

        if (hnd == null)
            throw new IgniteException("Failed to process incoming message (received message before compression " +
                "handler was created): " + ses);

        return hnd;
    }

    /**
     * Checks type of the message passed to the filter and converts it to a byte buffer (since compression filter
     * operates only on binary data).
     *
     * @param ses Session instance.
     * @param msg Message passed in.
     * @return Message that was cast to a byte buffer.
     * @throws GridNioException If msg is not a byte buffer.
     */
    private ByteBuffer checkMessage(GridNioSession ses, Object msg) throws GridNioException {
        if (!(msg instanceof ByteBuffer))
            throw new GridNioException("Invalid object type received (is compress filter correctly placed in filter " +
                "chain?) [ses=" + ses + ", msgClass=" + msg.getClass().getName() +  ']');

        return (ByteBuffer)msg;
    }
}
