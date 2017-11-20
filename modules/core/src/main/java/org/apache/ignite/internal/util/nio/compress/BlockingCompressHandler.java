package org.apache.ignite.internal.util.nio.compress;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.nio.GridNioException;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.util.nio.compress.CompressEngineResult.BUFFER_OVERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressEngineResult.BUFFER_UNDERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressEngineResult.OK;

/** */
public class BlockingCompressHandler {
    /** Logger. */
    private IgniteLogger log;

    /** Order. */
    private final ByteOrder order;

    /** Compress engine. */
    private final CompressEngine compressEngine;

    /** Output buffer into which encrypted data will be written. */
    private ByteBuffer outNetBuf;

    /** Input buffer from which Compress engine will decrypt data. */
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

        // Allocate a little bit more so compress engine would not return buffer overflow status.
        //
        // System property override is for test purposes only.
        int netBufSize = Integer.getInteger("BlockingCompressHandler.netBufSize",
            100000 + 50);

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
     * Encrypts data to be written to the network.
     *
     * @param src data to encrypt.
     * @throws IOException on errors.
     * @return Output buffer with encrypted data.
     */
    public ByteBuffer encrypt(ByteBuffer src) throws IOException {
        // The data buffer is (must be) empty, we can reuse the entire
        // buffer.
        outNetBuf.clear();

        // Loop until there is no more data in src
        while (src.hasRemaining()) {
            int outNetRemaining = outNetBuf.capacity() - outNetBuf.position();

            if (outNetRemaining < src.remaining() * 2) {
                outNetBuf = expandBuffer(outNetBuf, Math.max(
                    outNetBuf.position() + src.remaining() * 2, outNetBuf.capacity() * 2));

                if (log.isDebugEnabled())
                    log.debug("Expanded output net buffer: " + outNetBuf.capacity());
            }

            CompressEngineResult res = compressEngine.wrap(src, outNetBuf);


            if (res != OK)
                throw new IOException("Failed to encrypt data (compress engine error) [status=" + res+']');
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

        if (isInboundDone()) {
            int newPosition = buf.position() - inNetBuf.position();

            if (newPosition >= 0) {
                buf.position(newPosition);

                // If we received close_notify but not all bytes has been read by compress engine, print a warning.
                if (buf.hasRemaining())
                    U.warn(log, "Got unread bytes after receiving close_notify message (will ignore).");
            }

            inNetBuf.clear();
        }

        appBuf.flip();

        return appBuf;
    }

    /**
     * @return {@code True} if inbound data stream has ended
     */
    private boolean isInboundDone() {
        return compressEngine.isInboundDone();
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

        CompressEngineResult res = unwrap0();

        // prepare to be written again
        inNetBuf.compact();

        checkStatus(res);
    }

    /**
     * Performs raw unwrap from network read buffer.
     *
     * @return Result.
     * @throws IOException If compress exception occurs.
     */
    private CompressEngineResult unwrap0() throws IOException {
        CompressEngineResult res;

        do {
            res = compressEngine.unwrap(inNetBuf, appBuf);

            if (res == BUFFER_OVERFLOW)
                appBuf = expandBuffer(appBuf, appBuf.capacity() * 2);
        }
        while (res == OK || res == BUFFER_OVERFLOW);

        return res;
    }

    /**
     * @param res Compress engine result.
     * @throws IOException If status is not acceptable.
     */
    private void checkStatus(CompressEngineResult res)
        throws IOException {

        if (res != OK && res != BUFFER_UNDERFLOW)
            throw new IOException("Failed to unwrap incoming data (Compress engine error). Status: " + res);
    }

    /**
     * Allocate application buffer.
     */
    private ByteBuffer allocateAppBuff() {
        int netBufSize = 100000 + 50;

        int appBufSize = Math.max(100000 + 50, netBufSize * 2);

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
