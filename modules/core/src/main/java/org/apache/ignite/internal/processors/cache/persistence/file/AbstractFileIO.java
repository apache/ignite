package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public abstract class AbstractFileIO implements FileIO {
    /** Max io timeout milliseconds. */
    private static final int MAX_IO_TIMEOUT_MS = 2000;

    /**
     *
     */
    private interface IOOperation {
        /**
         * @param offs Offset.
         *
         * @return Number of bytes operated.
         */
        public int run(int offs) throws IOException;
    }

    /**
     * @param operation IO operation.
     *
     * @param num Number of bytes to operate.
     */
    private int fully(IOOperation operation, int num, boolean write) throws IOException {
        if (num > 0) {
            long time = 0;

            for (int i = 0; i < num; ) {
                int n = operation.run(i);

                if (n > 0) {
                    i += n;
                    time = 0;
                }
                else if (n == 0) {
                    if (time == 0)
                        time = U.currentTimeMillis();
                    else if ((U.currentTimeMillis() - time) >= MAX_IO_TIMEOUT_MS)
                        throw new IOException(write && position() == size() ? "Failed to extend file." :
                            "Probably disk is too busy, please check your device.");
                }
                else
                    throw new EOFException("EOF at position [" + position() + "] expected to read [" + num + "] bytes.");
            }
        }

        return num;
    }

    /** {@inheritDoc} */
    @Override public int readFully(final ByteBuffer destBuf) throws IOException {
        return fully(new IOOperation() {
            @Override public int run(int offs) throws IOException {
                return read(destBuf);
            }
        }, avail(destBuf.remaining()), false);
    }

    /** {@inheritDoc} */
    @Override public int readFully(final ByteBuffer destBuf, final long position) throws IOException {
        return fully(new IOOperation() {
            @Override public int run(int offs) throws IOException {
                return read(destBuf, position + offs);
            }
        }, avail(destBuf.remaining(), position), false);
    }

    /** {@inheritDoc} */
    @Override public int readFully(final byte[] buf, final int off, final int len) throws IOException {
        return fully(new IOOperation() {
            @Override public int run(int offs) throws IOException {
                return read(buf, off + offs, len - offs);
            }
        }, len, false);
    }

    /** {@inheritDoc} */
    @Override public int writeFully(final ByteBuffer srcBuf) throws IOException {
        return fully(new IOOperation() {
            @Override public int run(int offs) throws IOException {
                return write(srcBuf);
            }
        }, srcBuf.remaining(), true);
    }

    /** {@inheritDoc} */
    @Override public int writeFully(final ByteBuffer srcBuf, final long position) throws IOException {
        return fully(new IOOperation() {
            @Override public int run(int offs) throws IOException {
                return write(srcBuf, position + offs);
            }
        }, srcBuf.remaining(), true);
    }

    /** {@inheritDoc} */
    @Override public int writeFully(final byte[] buf, final int off, final int len) throws IOException {
        return fully(new IOOperation() {
            @Override public int run(int offs) throws IOException {
                return write(buf, off + offs, len - offs);
            }
        }, len, true);
    }

    /**
     * @param requested Requested.
     */
    private int avail(int requested) throws IOException {
        return avail(requested, position());
    }

    /**
     * @param requested Requested.
     * @param position Position.
     */
    private int avail(int requested, long position) throws IOException {
        long avail = size() - position;

        return requested > avail ? (int) avail : requested;
    }

}
