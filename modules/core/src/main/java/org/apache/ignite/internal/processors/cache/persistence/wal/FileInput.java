/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.IgniteDataIntegrityViolationException;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.PureJavaCrc32;
import org.jetbrains.annotations.NotNull;

/**
 * TODO: Add interface description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface FileInput extends ByteBufferBackedDataInput {

    FileIO io();

    void seek(long pos) throws IOException;

    long position();

    SimpleFileInput.Crc32CheckingFileInput startRead(boolean skipCheck);

    /**
     * Checking of CRC32.
     */
    public class Crc32CheckingFileInput implements ByteBufferBackedDataInput, AutoCloseable {
        /** */
        private final PureJavaCrc32 crc32 = new PureJavaCrc32();

        /** Last calc position. */
        private int lastCalcPosition;

        /** Skip crc check. */
        private boolean skipCheck;

        private FileInput delegate;

        /**
         */
        public Crc32CheckingFileInput(FileInput delegate, boolean skipCheck) {
            this.delegate = delegate;
            this.lastCalcPosition = delegate.buffer().position();
            this.skipCheck = skipCheck;
        }

        /** {@inheritDoc} */
        @Override public void ensure(int requested) throws IOException {
            int available = buffer().remaining();

            if (available >= requested)
                return;

            updateCrc();

            delegate.ensure(requested);

            lastCalcPosition = 0;
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            updateCrc();

            int val = crc32.getValue();

            int writtenCrc =  this.readInt();

            if ((val ^ writtenCrc) != 0 && !skipCheck) {
                // If it last message we will skip it (EOF will be thrown).
                ensure(5);

                throw new IgniteDataIntegrityViolationException(
                    "val: " + val + " writtenCrc: " + writtenCrc
                );
            }
        }

        /**
         *
         */
        private void updateCrc() {
            if (skipCheck)
                return;

            int oldPos = buffer().position();

            buffer().position(lastCalcPosition);

            crc32.update(delegate.buffer(), oldPos - lastCalcPosition);

            lastCalcPosition = oldPos;
        }

        /** {@inheritDoc} */
        @Override public int skipBytes(int n) throws IOException {
            ensure(n);

            int skipped = Math.min(buffer().remaining(), n);

            buffer().position(buffer().position() + skipped);

            return skipped;
        }

        /**
         * {@inheritDoc}
         */
        @Override public void readFully(@NotNull byte[] b) throws IOException {
            ensure(b.length);

            buffer().get(b);
        }

        /**
         * {@inheritDoc}
         */
        @Override public void readFully(@NotNull byte[] b, int off, int len) throws IOException {
            ensure(len);

            buffer().get(b, off, len);
        }

        /**
         * {@inheritDoc}
         */
        @Override public boolean readBoolean() throws IOException {
            return readByte() == 1;
        }

        /**
         * {@inheritDoc}
         */
        @Override public byte readByte() throws IOException {
            ensure(1);

            return buffer().get();
        }

        /**
         * {@inheritDoc}
         */
        @Override public int readUnsignedByte() throws IOException {
            return readByte() & 0xFF;
        }

        /**
         * {@inheritDoc}
         */
        @Override public short readShort() throws IOException {
            ensure(2);

            return buffer().getShort();
        }

        /**
         * {@inheritDoc}
         */
        @Override public int readUnsignedShort() throws IOException {
            return readShort() & 0xFFFF;
        }

        /**
         * {@inheritDoc}
         */
        @Override public char readChar() throws IOException {
            ensure(2);

            return buffer().getChar();
        }

        /**
         * {@inheritDoc}
         */
        @Override public int readInt() throws IOException {
            ensure(4);

            return buffer().getInt();
        }

        /**
         * {@inheritDoc}
         */
        @Override public long readLong() throws IOException {
            ensure(8);

            return buffer().getLong();
        }

        /**
         * {@inheritDoc}
         */
        @Override public float readFloat() throws IOException {
            ensure(4);

            return buffer().getFloat();
        }

        /**
         * {@inheritDoc}
         */
        @Override public double readDouble() throws IOException {
            ensure(8);

            return buffer().getDouble();
        }

        /**
         * {@inheritDoc}
         */
        @Override public String readLine() throws IOException {
            throw new UnsupportedOperationException();
        }

        /**
         * {@inheritDoc}
         */
        @Override public String readUTF() throws IOException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public ByteBuffer buffer() {
            return delegate.buffer();
        }
    }
}
