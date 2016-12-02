package org.apache.ignite.internal.processors.hadoop.shuffle.collections;

import java.io.DataInput;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.hadoop.HadoopSerialization;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.shuffle.mem.MemoryManager;
import org.apache.ignite.internal.processors.hadoop.shuffle.streams.HadoopDataOutStream;
import org.apache.ignite.internal.processors.hadoop.shuffle.streams.HadoopOffheapBuffer;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for adders.
 */
public abstract class AdderBase implements HadoopMultimap.Adder {
    /** */
    protected final MemoryManager mem;
    
    /** */
    protected final HadoopSerialization keySer;

    /** */
    protected final HadoopSerialization valSer;

    /** */
    private final HadoopDataOutStream out;

    /** */
    private long writeStart;

    /**
     * @param ctx Task context.
     * @param mem Memory manager.
     * @throws IgniteCheckedException If failed.
     */
    protected AdderBase(HadoopTaskContext ctx, MemoryManager mem) throws IgniteCheckedException {
        this.mem = mem;

        valSer = ctx.valueSerialization();
        keySer = ctx.keySerialization();

        out = new HadoopDataOutStream(mem) {
            @Override public long move(long size) {
                long ptr = super.move(size);

                if (ptr == 0) // Was not able to move - not enough free space.
                    ptr = allocateNextPage(size);

                assert ptr != 0;

                return ptr;
            }
        };
    }

    /**
     * @param requestedSize Requested size.
     * @return Next write pointer.
     */
    private long allocateNextPage(long requestedSize) {
        int writtenSize = writtenSize();

        long newPageSize = nextPageSize(writtenSize + requestedSize);
        long newPagePtr = mem.allocate(newPageSize);

        HadoopOffheapBuffer b = out.buffer();

        b.set(newPagePtr, newPageSize);

        if (writtenSize != 0) {
            mem.copyMemory(writeStart, newPagePtr, writtenSize);

            b.move(writtenSize);
        }

        writeStart = newPagePtr;

        return b.move(requestedSize);
    }

    /**
     * Get next page size.
     *
     * @param required Required amount of data.
     * @return Next page size.
     */
    private long nextPageSize(long required) {
        long pages = (required / mem.pageSize()) + 1;

        long pagesPow2 = nextPowerOfTwo(pages);

        return pagesPow2 * mem.pageSize();
    }

    /**
     * Get next power of two which greater or equal to the given number. Naive implementation.
     *
     * @param val Number
     * @return Nearest pow2.
     */
    private long nextPowerOfTwo(long val) {
        long res = 1;

        while (res < val)
            res = res << 1;

        if (res < 0)
            throw new IllegalArgumentException("Value is too big to find positive pow2: " + val);

        return res;
    }

    /**
     * @return Fixed pointer.
     */
    private long fixAlignment() {
        HadoopOffheapBuffer b = out.buffer();

        long ptr = b.pointer();

        if ((ptr & 7L) != 0) { // Address is not aligned by octet.
            ptr = (ptr + 8L) & ~7L;

            b.pointer(ptr);
        }

        return ptr;
    }

    /**
     * @param off Offset.
     * @param ser Serialization.
     * @param o Object.
     * @return Page pointer.
     * @throws IgniteCheckedException If failed.
     */
    protected long write(int off, Object o, HadoopSerialization ser) throws IgniteCheckedException {
        writeStart = fixAlignment();

        if (off != 0)
            out.move(off);

        ser.write(out, o);

        return writeStart;
    }

    /**
     * @param size Size.
     * @return Pointer.
     */
    protected long allocate(int size) {
        writeStart = fixAlignment();

        out.move(size);

        return writeStart;
    }

    /**
     * Rewinds local allocation pointer to the given pointer if possible.
     *
     * @param ptr Pointer.
     */
    protected void localDeallocate(long ptr) {
        HadoopOffheapBuffer b = out.buffer();

        if (b.isInside(ptr))
            b.pointer(ptr);
        else
            b.reset();
    }

    /**
     * @return Written size.
     */
    protected int writtenSize() {
        return (int)(out.buffer().pointer() - writeStart);
    }

    /** {@inheritDoc} */
    @Override public HadoopMultimap.Key addKey(DataInput in, @Nullable HadoopMultimap.Key reuse) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteCheckedException {
        keySer.close();
        valSer.close();
    }
}
