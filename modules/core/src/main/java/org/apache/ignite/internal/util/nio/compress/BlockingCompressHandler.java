package org.apache.ignite.internal.util.nio.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.GridNioException;

import static org.apache.ignite.internal.util.nio.compress.CompressEngineResult.BUFFER_OVERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressEngineResult.OK;

/** */
public class BlockingCompressHandler {
    /** Logger. */
    private IgniteLogger log;

    /** Order. */
    private final ByteOrder order;

    /** Compress engine. */
    private final CompressEngine compressEngine;

    /** Output buffer into which compressed data will be written. */
    private ByteBuffer outNetBuf;

    /** Input buffer from which compress engine will decrypt data. */
    private ByteBuffer inNetBuf;

    /** Application buffer. */
    private ByteBuffer appBuf;

    /**
     * @param compressEngine compressEngine.
     * @param directBuf Direct buffer flag.
     * @param order Byte order.
     * @param log Logger.
     */
    public BlockingCompressHandler(CompressEngine compressEngine,
        boolean directBuf,
        ByteOrder order,
        IgniteLogger log)
        throws IOException {
        this.log = log;
        this.compressEngine = compressEngine;
        this.order = order;

        int netBufSize = 32768;

        outNetBuf = directBuf ? ByteBuffer.allocateDirect(netBufSize) : ByteBuffer.allocate(netBufSize);
        outNetBuf.order(order);

        // Initially buffer is empty.
        outNetBuf.position(0);
        outNetBuf.limit(0);

        inNetBuf = directBuf ? ByteBuffer.allocateDirect(netBufSize) : ByteBuffer.allocate(netBufSize);
        inNetBuf.order(order);

        appBuf = allocateAppBuff();

        if (log.isDebugEnabled())
            log.debug("Started compress session [netBufSize=" + netBufSize + ", appBufSize=" + appBuf.capacity() + ']');
    }

    /** */
    public ByteBuffer inputBuffer(){
        return inNetBuf;
    }

    /**
     * @return Application buffer with decoded data.
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
            CompressEngineResult res = compressEngine.wrap(src, outNetBuf);

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
     * Called by compress filter when new message was received.
     *
     * @param buf Received message.
     * @throws GridNioException If exception occurred while forwarding events to underlying filter.
     * @throws IOException If failed to process compress data.
     */
    public ByteBuffer decode(ByteBuffer buf) throws IgniteCheckedException, IOException {
        appBuf.clear();

        if (buf.limit() > inNetBuf.remaining()) {
            inNetBuf = expandBuffer(inNetBuf, inNetBuf.capacity() + buf.limit() * 2);

            appBuf = expandBuffer(appBuf, inNetBuf.capacity() * 2);

            if (log.isDebugEnabled())
                log.debug("Expanded buffers [inNetBufCapacity=" + inNetBuf.capacity() + ", appBufCapacity=" +
                    appBuf.capacity() + ']');
        }

        // append buf to inNetBuffer
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

        CompressEngineResult res;

        do {
            res = compressEngine.unwrap(inNetBuf, appBuf);

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
        int netBufSize = 32768;

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
