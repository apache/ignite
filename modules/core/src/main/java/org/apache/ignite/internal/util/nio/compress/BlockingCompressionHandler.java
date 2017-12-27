package org.apache.ignite.internal.util.nio.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.GridNioException;

import static org.apache.ignite.internal.util.nio.compress.CompressionEngineResult.BUFFER_OVERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressionEngineResult.OK;

/**
 *
 */
public class BlockingCompressionHandler {
    /** Size of a net buffers. */
    private static final int netBufSize = 32768;

    /** Logger. */
    private IgniteLogger log;

    /** Order. */
    private final ByteOrder order;

    /** Compress engine. */
    private final CompressionEngine compressionEngine;

    /** Output buffer into which compressed data will be written. */
    private ByteBuffer outNetBuf;

    /** Input buffer from which compression engine will decompress data. */
    private ByteBuffer inNetBuf;

    /** Application buffer. */
    private ByteBuffer appBuf;

    /**
     * @param compressionEngine compressionEngine.
     * @param directBuf Direct buffer flag.
     * @param order Byte order.
     * @param log Logger.
     */
    public BlockingCompressionHandler(CompressionEngine compressionEngine,
        boolean directBuf,
        ByteOrder order,
        IgniteLogger log)
        throws IOException {
        this.log = log;
        this.compressionEngine = compressionEngine;
        this.order = order;

        outNetBuf = directBuf ? ByteBuffer.allocateDirect(netBufSize) : ByteBuffer.allocate(netBufSize);
        outNetBuf.order(order);

        // Initially buffer is empty.
        outNetBuf.position(0);
        outNetBuf.limit(0);

        inNetBuf = directBuf ? ByteBuffer.allocateDirect(netBufSize) : ByteBuffer.allocate(netBufSize);
        inNetBuf.order(order);

        appBuf = allocateAppBuff();

        if (log.isDebugEnabled())
            log.debug("Started compression session [netBufSize=" + netBufSize + ", appBufSize=" + appBuf.capacity() + ']');
    }

    /** */
    public ByteBuffer inputBuffer(){
        return inNetBuf;
    }

    /**
     * @return Application buffer with decompressed data.
     */
    public ByteBuffer applicationBuffer() {
        appBuf.flip();

        return appBuf;
    }

    /**
     * Compress data to be written to the network.
     *
     * @param src data to compress.
     * @throws IOException on errors.
     * @return Output buffer with compressed data.
     */
    public ByteBuffer compress(ByteBuffer src) throws IOException {
        // The data buffer is (must be) empty, we can reuse the entire
        // buffer.
        outNetBuf.clear();

        // Loop until there is no more data in src
        while (src.hasRemaining()) {
            CompressionEngineResult res = compressionEngine.wrap(src, outNetBuf);

            if (res == BUFFER_OVERFLOW) {
                outNetBuf = expandBuffer(outNetBuf, Math.max(
                    outNetBuf.position() + src.remaining() * 2, outNetBuf.capacity() * 2));

                if (log.isDebugEnabled())
                    log.debug("Expanded output net buffer [outNetBufCapacity=" + outNetBuf.capacity());
            }
        }

        outNetBuf.flip();

        return outNetBuf;
    }

    /**
     * Called by compression filter when new message was received.
     *
     * @param buf Received message.
     * @throws GridNioException If exception occurred while forwarding events to underlying filter.
     * @throws IOException If failed to process compress data.
     */
    public ByteBuffer decompress(ByteBuffer buf) throws IgniteCheckedException, IOException {
        appBuf.clear();

        if (buf.limit() > inNetBuf.remaining()) {
            inNetBuf = expandBuffer(inNetBuf, inNetBuf.capacity() + buf.limit() * 2);

            if (log.isDebugEnabled())
                log.debug("Expanded buffer [inNetBufCapacity=" + inNetBuf.capacity() + ']');
        }

        inNetBuf.put(buf);

        unwrapData();

        appBuf.flip();

        return appBuf;
    }

    /**
     * Unwraps user data to the application buffer.
     *
     * @throws IOException If failed to process compress data.
     * @throws GridNioException If failed to pass events to the next filter.
     */
    private void unwrapData() throws IgniteCheckedException, IOException {
        if (log.isDebugEnabled())
            log.debug("Unwrapping received data.");

        // Flip buffer so we can read it.
        inNetBuf.flip();

        CompressionEngineResult res;

        do {
            res = compressionEngine.unwrap(inNetBuf, appBuf);

            if (res == BUFFER_OVERFLOW)
                appBuf = expandBuffer(appBuf, appBuf.capacity() * 2);
        }
        while (res == OK || res == BUFFER_OVERFLOW);

        // prepare to be written again
        inNetBuf.compact();
    }

    /**
     * Allocate application buffer.
     */
    private ByteBuffer allocateAppBuff() {
        int appBufSize = netBufSize * 2;

        ByteBuffer buf = ByteBuffer.allocate(appBufSize);

        buf.order(order);

        return buf;
    }

    /**
     * Expands the given byte buffer to the requested capacity.
     *
     * @param original Original byte buffer.
     * @param cap Requested capacity.
     * @return Expanded byte buffer.
     */
    private ByteBuffer expandBuffer(ByteBuffer original, int cap) {
        ByteBuffer res = original.isDirect() ? ByteBuffer.allocateDirect(cap) : ByteBuffer.allocate(cap);

        res.order(original.order());

        original.flip();

        res.put(original);

        return res;
    }
}
