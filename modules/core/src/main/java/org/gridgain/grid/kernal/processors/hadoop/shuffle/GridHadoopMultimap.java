/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle;

import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Multimap for map reduce intermediate results.
 */
public class GridHadoopMultimap implements AutoCloseable {
    /** Current hash table capacity. */
    private int tblCap = 1024;

    /** Pointer to hash table. */
    private long tblPtr;

    /** */
    private final GridHadoopJob job;

    /** */
    private final GridUnsafeMemory mem;

    /** */
    private final GridHadoopAllocator malloc;

    /**
     * @param job Job.
     * @param mem Memory.
     */
    public GridHadoopMultimap(GridHadoopJob job, GridUnsafeMemory mem, int tblCap) {
        assert U.isPow2(tblCap);

        this.job = job;
        this.mem = mem;
        this.tblCap = tblCap;

        malloc = new GridHadoopAllocator(mem);

        tblPtr = mem.allocate(8 * tblCap, true, false);
    }

    /**
     * @return Adder object.
     */
    public Adder startAdding() throws GridException {
        return new Adder();
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        // TODO
    }

    public class Adder implements AutoCloseable {
        /** */
        private Object tmpKey;

        /** */
        private final GridHadoopSerialization serialization;

        /** */
        private GridHadoopDataOutStream metaOut;

        /** */
        private GridHadoopDataOutStream keyOut;

        /** */
        private GridHadoopDataOutStream valOut;

        /**
         * @throws GridException If failed.
         */
        public Adder() throws GridException {
            serialization = job.serialization();

            metaOut = newOutStream();
            keyOut = newOutStream();
            valOut = newOutStream();
        }

        private GridHadoopDataOutStream newOutStream() {
            return new GridHadoopDataOutStream(mem, malloc.nextPage(), GridHadoopAllocator.PAGE_SIZE) {
                @Override protected long move(long size) throws IOException {
                    long oldPtr = super.move(size);

                    if (oldPtr == 0) {
                        // TODO
                    }

                    return super.move(size);
                }
            };
        }

        /**
         * Adds value for the given key.
         *
         * @param key Key.
         * @param val Value.
         * @return Key page pointer.
         */
        public long add(Object key, Object val) {
            int keyHash = U.hash(key.hashCode());

            long tblAddr = 8 * (keyHash & (tblCap - 1));

            for (;;) {
                long metaPtr = mem.readLongVolatile(tblAddr);

                if (metaPtr == 0) {
                    long valPtr = write(valOut, val, 0);
                    long keyPtr = write(keyOut, key, valPtr);

                    metaPtr = writeMeta(keyPtr, 0, 0, keyHash, (int) (keyOut.buffer().pointer() - keyPtr));

                    if (mem.casLong())
                }
                else {

                }
            }

            return 0;
        }

        private long writeMeta(long keyPtr, long collisionPtr, long lastSentVal, int keyHash, int keySize) {
            if (metaOut.buffer().remaining() < 32) {
                // TODO switch buf
            }

            long metaPtr = metaOut.buffer().pointer();

            try {
                metaOut.writeLong(keyPtr);
                metaOut.writeLong(collisionPtr);
                metaOut.writeLong(lastSentVal);
                metaOut.writeInt(keyHash);
                metaOut.writeInt(keySize);
            }
            catch (IOException e) {
                throw new IllegalStateException(e);
            }

            return metaPtr;
        }

        private long write(GridHadoopDataOutStream out, Object obj, long nextPtr) {
            GridHadoopBuffer b = out.buffer();

            long buf0 = b.buffer();
            long ptr = b.pointer();

            out.writeLong(nextPtr);
            serialization.write(out, obj);

            int hill = (int)(b.pointer() & 7);

            if (hill != 0) {
                long res = b.move(8 - hill);

                assert res != 0;
            }

            long buf1 = b.buffer();

            return buf0 == buf1 ? ptr : buf1; // If buffer was changed internally we'll return begin of new buffer.
        }

        @Override public void close()  {
            // TODO
        }
    }
}
