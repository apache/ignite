package org.apache.ignite.internal.processors.hadoop.shuffle.collections;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.hadoop.HadoopSerialization;
import org.apache.ignite.internal.processors.hadoop.shuffle.mem.MemoryManager;
import org.apache.ignite.internal.processors.hadoop.shuffle.streams.HadoopDataInStream;

/**
 * Reader for key and value.
 */
class ReaderBase implements AutoCloseable {
    /** */
    private Object tmp;

    /** */
    private final HadoopSerialization ser;

    /** */
    private final HadoopDataInStream in;

    /** */
    private final MemoryManager mem;

    /**
     * @param ser Serialization.
     * @param mem Memory manger.
     */
    protected ReaderBase(HadoopSerialization ser, MemoryManager mem) {
        assert ser != null;
        assert mem != null;

        this.ser = ser;
        this.mem = mem;

        in = new HadoopDataInStream(mem);
    }

    /**
     * @param valPtr Value page pointer.
     * @return Value.
     */
    public Object readValue(long valPtr) {
        assert valPtr > 0 : valPtr;

        try {
            return read(valPtr + 12, mem.valueSize(valPtr));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Resets temporary object to the given one.
     *
     * @param tmp Temporary object for reuse.
     */
    public void resetReusedObject(Object tmp) {
        this.tmp = tmp;
    }

    /**
     * @param ptr Pointer.
     * @param size Object size.
     * @return Object.
     * @throws IgniteCheckedException If failed.
     */
    protected Object read(long ptr, long size) throws IgniteCheckedException {
        in.buffer().set(ptr, size);

        tmp = ser.read(in, tmp);

        return tmp;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteCheckedException {
        ser.close();
    }
}
