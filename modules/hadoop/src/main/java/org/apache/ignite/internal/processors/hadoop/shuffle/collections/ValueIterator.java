package org.apache.ignite.internal.processors.hadoop.shuffle.collections;

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.ignite.internal.processors.hadoop.shuffle.mem.MemoryManager;

/**
 * Iterator over values.
 */
class ValueIterator implements Iterator<Object> {
    /** */
    private long valPtr;

    /** */
    private final ReaderBase valReader;

    /** */
    private final MemoryManager mem;

    /**
     * @param valPtr Value page pointer.
     * @param valReader Value reader.
     * @param mem Memory manager.
     */
    protected ValueIterator(long valPtr, ReaderBase valReader, MemoryManager mem) {
        this.valPtr = valPtr;
        this.valReader = valReader;
        this.mem = mem;
    }

    /**
     * @param valPtr Head value pointer.
     */
    public void head(long valPtr) {
        this.valPtr = valPtr;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return valPtr != 0;
    }

    /** {@inheritDoc} */
    @Override public Object next() {
        if (!hasNext())
            throw new NoSuchElementException();

        Object res = valReader.readValue(valPtr);

        valPtr = mem.nextValue(valPtr);

        return res;
    }

    /** {@inheritDoc} */
    @Override public void remove() {
        throw new UnsupportedOperationException();
    }
}
