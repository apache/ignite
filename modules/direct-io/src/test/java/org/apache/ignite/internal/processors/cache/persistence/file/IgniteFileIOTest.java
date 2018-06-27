package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import junit.framework.TestCase;
import org.apache.ignite.internal.util.GridUnsafe;
import org.jetbrains.annotations.NotNull;

/**
 * File IO tests.
 */
public class IgniteFileIOTest extends TestCase {
    /** Test data size. */
    private static final int TEST_DATA_SIZE = 16 * 1024 * 1024;

    /**
     *
     */
    private static class TestFileIO extends AbstractFileIO {
        /** Data. */
        private final byte[] data;
        /** Position. */
        private int position;

        /**
         * @param maxSize Maximum size.
         */
        TestFileIO(int maxSize) {
            this.data = new byte[maxSize];
        }

        /**
         * @param data Initial data.
         */
        TestFileIO(byte[] data) {
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public long position() throws IOException {
            return position;
        }

        /** {@inheritDoc} */
        @Override public void position(long newPosition) throws IOException {
            checkPosition(newPosition);

            this.position = (int)newPosition;
        }

        /** {@inheritDoc} */
        @Override public int read(ByteBuffer destBuf) throws IOException {
            final int len = Math.min(destBuf.remaining(), data.length - position);

            destBuf.put(data, position, len);

            position += len;

            return len;
        }

        /** {@inheritDoc} */
        @Override public int read(ByteBuffer destBuf, long position) throws IOException {
            checkPosition(position);

            final int len = Math.min(destBuf.remaining(), data.length - (int)position);

            destBuf.put(data, (int)position, len);

            return len;
        }

        /** {@inheritDoc} */
        @Override public int read(byte[] buf, int off, int maxLen) throws IOException {
            final int len = Math.min(maxLen, data.length - position);

            System.arraycopy(data, position, buf, off, len);

            position += len;

            return len;
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer srcBuf) throws IOException {
            final int len = Math.min(srcBuf.remaining(), data.length - position);

            srcBuf.get(data, position, len);

            position += len;

            return len;
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
            checkPosition(position);

            final int len = Math.min(srcBuf.remaining(), data.length - (int)position);

            srcBuf.get(data, (int)position, len);

            return len;
        }

        /** {@inheritDoc} */
        @Override public int write(byte[] buf, int off, int maxLen) throws IOException {
            final int len = Math.min(maxLen, data.length - position);

            System.arraycopy(buf, off, data, position, len);

            position += len;

            return len;
        }

        /** {@inheritDoc} */
        @Override public MappedByteBuffer map(int sizeBytes) throws IOException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void force() throws IOException {
        }

        /** {@inheritDoc} */
        @Override public void force(boolean withMetadata) throws IOException {
        }

        /** {@inheritDoc} */
        @Override public long size() throws IOException {
            return data.length;
        }

        /** {@inheritDoc} */
        @Override public void clear() throws IOException {
            position = 0;
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
        }

        /**
         * @param position Position.
         */
        private void checkPosition(long position) throws IOException {
            if (position < 0 || position >= data.length)
                throw new IOException("Invalid position: " + position);
        }
    }

    /**
     * test for 'full read' functionality.
     */
    public void testReadFully() throws Exception {
        byte[] arr = new byte[TEST_DATA_SIZE];

        fillRandomArray(arr);

        ByteBuffer buf = ByteBuffer.allocate(TEST_DATA_SIZE);

        TestFileIO fileIO = new TestFileIO(arr) {
            @Override public int read(ByteBuffer destBuf) throws IOException {
                if (destBuf.remaining() < 2)
                    return super.read(destBuf);

                int oldLimit = destBuf.limit();

                destBuf.limit(destBuf.position() + (destBuf.remaining() >> 1));

                try {
                    return super.read(destBuf);
                }
                finally {
                    destBuf.limit(oldLimit);
                }
            }
        };

        fileIO.readFully(buf);

        assert buf.remaining() == 0;

        assert compareArrays(arr, buf.array());
    }

    /**
     * test for 'full write' functionality.
     */
    public void testWriteFully() throws Exception {
        byte[] arr = new byte[TEST_DATA_SIZE];

        ByteBuffer buf = ByteBuffer.allocate(TEST_DATA_SIZE);

        fillRandomArray(buf.array());

        TestFileIO fileIO = new TestFileIO(arr) {
            @Override public int write(ByteBuffer destBuf) throws IOException {
                if (destBuf.remaining() < 2)
                    return super.write(destBuf);

                int oldLimit = destBuf.limit();

                destBuf.limit(destBuf.position() + (destBuf.remaining() >> 1));

                try {
                    return super.write(destBuf);
                }
                finally {
                    destBuf.limit(oldLimit);
                }
            }
        };

        fileIO.writeFully(buf);

        assert buf.remaining() == 0;

        assert compareArrays(arr, buf.array());
    }

    /**
     *
     */
    private interface ArrayTask {

        /**
         * @param off Offset.
         * @param len Length.
         */
        public void process(int off, int len);

    }

    /**
     * @param arr Array.
     * @param task Task.
     */
    private static void processArray(@NotNull final byte[] arr, ArrayTask task) {
        if (arr.length == 0)
            return;

        ForkJoinPool srvc = ForkJoinPool.commonPool();

        final int blockSize = arr.length / srvc.getParallelism() + 1;

        ArrayList<ForkJoinTask<?>> tasks = new ArrayList<>(srvc.getParallelism() + 1);

        for (int i = 0; i < arr.length; i += blockSize) {
            final int len = Math.min(blockSize, arr.length - i);

            if (len > 0) {
                final int off = i;

                tasks.add(srvc.submit (() -> task.process(off, len)));
            }
        }

        for (ForkJoinTask<?> t : tasks)
            t.join();
    }

    /**
     * @param arr Array.
     */
    private static void fillRandomArray(@NotNull final byte[] arr) {
        ThreadLocalRandom.current().nextBytes(arr);
//        processArray(arr, (off, len) -> {
//            ThreadLocalRandom rnd = ThreadLocalRandom.current();
//
//            for (int j = 0; j < len >> 3; j++)
//                GridUnsafe.putLong(arr, GridUnsafe.BYTE_ARR_OFF + (j << 3) + off, rnd.nextLong());
//
//            for (int o = off + (len & ~7), j = 0; j < (len & 7); j++)
//                arr[o + j] = (byte) (rnd.nextInt(256) - 128);
//
//        });
    }

    /**
     * @param arr1 Array 1.
     * @param arr2 Array 2.
     */
    private static boolean compareArrays(@NotNull final byte[] arr1, @NotNull final byte[] arr2) {
        if (arr1.length != arr2.length)
            return false;

        for (int i = 0; i < arr1.length; i++)
            if (arr1[i] != arr2[i])
                return false;

        return true;

//        final AtomicBoolean res = new AtomicBoolean(true);
//
//        processArray(arr1, (off, len) -> {
//            for (int i = 0; i < len >> 3; i++) {
//                long o = GridUnsafe.BYTE_ARR_OFF +(i << 3) + off;
//
//                if (GridUnsafe.getLong(arr1, o) != GridUnsafe.getLong(arr2, o)) {
//                    res.set(false);
//
//                    return;
//                }
//
//                if (!res.get())
//                    return;
//            }
//
//            if (!res.get())
//                for (int o = off + (len & ~7), i = 0; i < (len & 7); i++)
//                    if (arr1[o + i] != arr2[o + i]) {
//                        res.set(false);
//
//                        return;
//                    }
//        });
//
//        return res.get();
    }
}
