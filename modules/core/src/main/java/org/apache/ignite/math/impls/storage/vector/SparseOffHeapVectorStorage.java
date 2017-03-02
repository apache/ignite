package org.apache.ignite.math.impls.storage.vector;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.stream.IntStream;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.math.VectorStorage;

/**
 * Sparse offheap vector storage.
 */
public class SparseOffHeapVectorStorage implements VectorStorage {
    private int size;
    private long ptr;

    /**
     * Externalization constructor.
     */
    public SparseOffHeapVectorStorage(){
        // No-op.
    }

    public SparseOffHeapVectorStorage(int size) {
        this.size = size;

        allocateMemory(size);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        return GridUnsafe.getDouble(pointerOffset(i));
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        GridUnsafe.putDouble(pointerOffset(i), v);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(size);

        for (int i = 0; i < size; i++)
            out.writeDouble(get(i));
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        size = in.readInt();

        allocateMemory(size);

        for (int i = 0; i < size; i++)
            set(i, in.readDouble());
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public double getLookupCost() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean isAddConstantTime() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() {
        GridUnsafe.freeMemory(ptr);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
        return obj != null && getClass().equals(obj.getClass()) && (size == ((SparseOffHeapVectorStorage)obj).size) &&
            (size == 0 || isMemoryEquals((SparseOffHeapVectorStorage)obj));
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int result = 1;

        result = result * 37 + size;
        result = result * 37 + Long.hashCode(ptr);

        return result;
    }

    /** */
    private boolean isMemoryEquals(SparseOffHeapVectorStorage otherStorage){
        return IntStream.range(0, size).parallel().noneMatch(idx -> Double.compare(get(idx), otherStorage.get(idx)) != 0);
    }

    /**
     * Pointer offset for specific index.
     *
     * @param i Offset index.
     */
    private long pointerOffset(int i) {
        return ptr + i * Double.BYTES;
    }

    /** */
    private void allocateMemory(int size) {
        ptr = GridUnsafe.allocateMemory(size * Double.BYTES);
    }
}
