/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.ignite.Ignition;
import lib.llpl.*;
import org.apache.ignite.internal.binary.BinarySchemaRegistry;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeListImpl;
import org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList;
import sun.nio.ch.DirectBuffer;

/**
 *
 *
 */
public class AepUnsafe extends GridUnsafe {

    /** A handle for AEP heap. */
    private Heap heap;

    /** The type of the AEP memory region. */
    private Class kind = Transactional.class;

    /** A persistent singly linked list. */
    private PersistentLinkedListOfLong<Transactional> persistentList;

    /** Holds the base address and the memory block. Key: base address, Value: reference to memory block. */
    private ConcurrentSkipListMap<Long, MemoryBlock<MemoryBlock.Kind>> segmentsMap;

    public enum BlockType { SEGMENT, BUCKET }

    private Lock casLock = new ReentrantLock();

    @SuppressWarnings("unchecked")
    public AepUnsafe() {
        heap = Ignition.getAepHeap();
        persistentList = new PersistentLinkedListOfLong<>(kind, heap);
        segmentsMap = new ConcurrentSkipListMap<>();
        if (heap.getRoot() != 0)
            loadPersistentStore();
        else
            createBinarySchemaRegion();
    }

    @SuppressWarnings("unchecked")
    private void loadPersistentStore() {
        persistentList.setRoot(heap.getRoot());
        MemoryBlock<MemoryBlock.Kind> block;

        // We skip index 0 (the schema registry region).
        int size = persistentList.size();
        for (int i = 1; i < size; i++) {
            Long base = persistentList.get(i);

            assert base != null;

            block = heap.memoryBlockFromAddress(kind, base);

            assert block != null;

            if (block.getInt(block.size() - 2 * Integer.BYTES) == BlockType.SEGMENT.ordinal())
                segmentsMap.put(base, block);
        }
    }

    @SuppressWarnings("unchecked")
    private void createBinarySchemaRegion() {
        MemoryBlock<?> block = heap.allocateMemoryBlock(kind, BinarySchemaRegistry.SCHEMA_REGISTRY_SIZE);
        persistentList.add(block.address());
        if (!Ignition.isAepClientModeEnabled())
            heap.setRoot(persistentList.getRoot());
    }

    /**
     * Allocates a persistent memory block for a segment. If we already have one allocated, we return it.
     *
     * @param size Size.
     * @return address.
     */
    @SuppressWarnings("unchecked")
    @Override
    public long allocateUnsafeMemory(String regionName, int index, long size) {
        int id = regionName.hashCode() + index;

        // We skip index 0 (the schema registry region).
        for (int i = 1; i < persistentList.size(); i++) {
            Long base = persistentList.get(i);

            assert base != null;

            MemoryBlock<MemoryBlock.Kind> block = heap.memoryBlockFromAddress(kind, base);

            if (id == block.getInt(block.size() - Integer.BYTES))
                return base;
        }

        MemoryBlock<MemoryBlock.Kind> block = heap.allocateMemoryBlock(kind, size + 2 * Integer.BYTES);
        block.setInt(size, BlockType.SEGMENT.ordinal());
        block.setInt(size + Integer.BYTES, id);

        // add chunk to persistent list
        long address = block.address();
        persistentList.add(address);

        // add list to skipListMap
        segmentsMap.put(address, block);

        return address;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public MemoryBlock<MemoryBlock.Kind> getSchemaRegistryBlock() {
        Long base = persistentList.get(0);

        assert base != null;

        return heap.memoryBlockFromAddress(kind, base);
    }

    @Override
    public PersistentLinkedList<Transactional, Long> getPersistentList() {
        return persistentList;
    }

    /**
     * Sets all bytes in a given block of memory to a copy of another block.
     *
     * @param srcObj Source object.
     * @param srcOff Source offset.
     * @param dstObj Dst object.
     * @param dstOff Dst offset.
     * @param len Length.
     */
    @Override
    public void copyMemory(Object srcObj, long srcOff, Object dstObj, long dstOff, long len) {

        if (len <= PER_BYTE_THRESHOLD && srcObj != null && dstObj != null) {
            for (int i = 0; i < len; i++)
                UNSAFE.putByte(dstObj, dstOff + i, UNSAFE.getByte(srcObj, srcOff + i));
            return;
        }

        Map.Entry<Long, MemoryBlock<MemoryBlock.Kind>> srcEntry = segmentsMap.floorEntry(srcOff);
        Map.Entry<Long, MemoryBlock<MemoryBlock.Kind>> dstEntry = segmentsMap.floorEntry(dstOff);

        MemoryBlock<MemoryBlock.Kind> src = srcEntry != null ? srcEntry.getValue() : null;
        MemoryBlock<MemoryBlock.Kind> dst = dstEntry != null ? dstEntry.getValue() : null;

        int mask = ((src == null ? 0 : 1) << 1) | (dst == null ? 0 : 1);

        switch (mask) {
            case 0:
                UNSAFE.copyMemory(srcObj, srcOff, dstObj, dstOff, len);
                break;
            case 1:
                for (int i = 0; i < len; i++)
                    dst.setByte(dstOff + i - dst.address(), UNSAFE.getByte(srcObj, srcOff + i));
                break;
            case 2:
                for (int i = 0; i < len; i++)
                    UNSAFE.putByte(dstObj, dstOff + i, src.getByte(srcOff + i - src.address()));
                break;
            default:
                copyMemory(srcOff, dstOff, len);
        }

    }

    /**
     * Copies len bytes from from src to dst.
     *
     * @param srcOff Source.
     * @param dstOff Dst.
     * @param len Length.
     */
    @Override
    public void copyMemory(long srcOff, long dstOff, long len) {
        MemoryBlock<MemoryBlock.Kind> src = segmentsMap.floorEntry(srcOff).getValue();
        MemoryBlock<MemoryBlock.Kind> dst = segmentsMap.floorEntry(dstOff).getValue();

        assert src != null && dst != null;

        dst.copyFromMemory(src, srcOff - src.address(), dstOff - dst.address(), len);
    }

    /**
     * Fills memory with the given value.
     *
     * @param address Address.
     * @param len Length.
     * @param val Value.
     */
    @Override
    public void setMemory(long address, long len, byte val) {
        MemoryBlock<MemoryBlock.Kind> r = segmentsMap.floorEntry(address).getValue();
        if (r != null)
            r.setMemory(val, address - r.address(), len);
        else
            UNSAFE.setMemory(address, len, val);
    }


    /**
     * De-allocate the memory region whose base address is address.
     *
     * @param address the base address of the memory region.
     */
    @Override
    public void freeMemoryBlock(long address) {
        segmentsMap.remove(address);
        persistentList.remove(address);
    }

    /**
     * De-allocates all allocated memory regions.
     */
    @Override
    public void freeMemoryBlocks() {
        persistentList.removeAll();
        segmentsMap.clear();
    }

    /**
     * We do not de-allocate on shutdown. Instead, we use it to persist FreelistImpl buckets.
     *
     * @param address
     */
    @Override
    public void freeUnsafeMemory(long address) {
        ConcurrentHashMap<Integer, AtomicReferenceArray<PagesList.Stripe[]>> bucketsMap = FreeListImpl.bucketsMap;
        for (Map.Entry<Integer, AtomicReferenceArray<PagesList.Stripe[]>> e : bucketsMap.entrySet()) {
            AtomicReferenceArray<PagesList.Stripe[]> ara = e.getValue();
            for (int i = 0; i < ara.length(); i++)
                FreeListImpl.persistStripes(e.getKey().hashCode(), i, ara.get(i));

            bucketsMap.remove(e.getKey());
        }
    }

    /**
     * We currently do not support a ByteBuffer over a persistent memory region.
     *
     * @param ptr Pointer to wrap.
     * @param len Memory location length.
     * @return Byte buffer wrapping the given memory.
     */
    @Override
    public ByteBuffer wrapPointer(long ptr, int len) {

        MemoryBlock<MemoryBlock.Kind> block = segmentsMap.floorEntry(ptr).getValue();

        assert block == null;

        ByteBuffer buf = nioAccess.newDirectByteBuffer(ptr, len, null);

        assert buf instanceof DirectBuffer;

        buf.order(NATIVE_BYTE_ORDER);

        return buf;
    }

    /**
     * Gets byte value from given address.
     *
     * @param address Address.
     * @return Byte value from given address.
     */
    @Override
    public byte getByte(long address) {
        MemoryBlock<MemoryBlock.Kind> r = segmentsMap.floorEntry(address).getValue();
        return r.getByte(address - r.address());
    }

    /**
     * Stores given byte value.
     *
     * @param address Address.
     * @param val Value.
     */
    @Override
    public void putByte(long address, byte val) {
        MemoryBlock<MemoryBlock.Kind> r = segmentsMap.floorEntry(address).getValue();
        r.setByte(address - r.address(), val);
    }

    /**
     * Gets char value from given address. Alignment aware.
     *
     * @param address Address.
     * @return Char value from given address.
     */
    @Override
    public char getChar(long address) {
        MemoryBlock<MemoryBlock.Kind> r = segmentsMap.floorEntry(address).getValue();
        return (char) r.getShort(address - r.address());
    }

    /**
     * Stores given char value. Alignment aware.
     *
     * @param address Address.
     * @param val Value.
     */
    @Override
    public void putChar(long address, char val) {
        MemoryBlock<MemoryBlock.Kind> r = segmentsMap.floorEntry(address).getValue();
        r.setShort(address - r.address(), (short) val);
    }

    /**
     * Gets short value from given address. Alignment aware.
     *
     * @param address Address.
     * @return Short value from given address.
     */
    @Override
    public short getShort(long address) {
        MemoryBlock<MemoryBlock.Kind> r = segmentsMap.floorEntry(address).getValue();
        return r.getShort(address - r.address());
    }

    /**
     * Stores given short value. Alignment aware.
     *
     * @param address Address.
     * @param val Value.
     */
    @Override
    public void putShort(long address, short val) {
        MemoryBlock<MemoryBlock.Kind> r = segmentsMap.floorEntry(address).getValue();
        r.setShort(address - r.address(), val);
    }

    /**
     * Gets integer value from given address. Alignment aware.
     *
     * @param address Address.
     * @return Integer value from given address.
     */
    @Override
    public int getInt(long address) {
        MemoryBlock<MemoryBlock.Kind> r = segmentsMap.floorEntry(address).getValue();
        return r.getInt(address - r.address());
    }

    /**
     * Stores given integer value. Alignment aware.
     *
     * @param address Address.
     * @param val Value.
     */
    @Override
    public void putInt(long address, int val) {
        MemoryBlock<MemoryBlock.Kind> r = segmentsMap.floorEntry(address).getValue();
        r.setInt(address - r.address(), val);
    }

    /**
     * Gets long value from given address. Alignment aware.
     *
     * @param address Address.
     * @return Long value from given address.
     */
    @Override
    public long getLong(long address) {
        MemoryBlock<MemoryBlock.Kind> r = segmentsMap.floorEntry(address).getValue();
        return r.getLong(address - r.address());
    }

    /**
     * Stores given integer value. Alignment aware.
     *
     * @param address Address.
     * @param val Value.
     */
    @Override
    public void putLong(long address, long val) {
        MemoryBlock<MemoryBlock.Kind> r = segmentsMap.floorEntry(address).getValue();
        r.setLong(address - r.address(), val);
    }

    /**
     * Gets float value from given address. Alignment aware.
     *
     * @param address Address.
     * @return Float value from given address.
     */
    @Override
    public float getFloat(long address) {
        return Float.intBitsToFloat(getInt(address));
    }

    /**
     * Stores given float value. Alignment aware.
     *
     * @param address Address.
     * @param val Value.
     */
    @Override
    public void putFloat(long address, float val) {
        putInt(address, Float.floatToRawIntBits(val));
    }

    /**
     * Gets double value from given address. Alignment aware.
     *
     * @param address Address.
     * @return Double value from given address.
     */
    @Override
    public double getDouble(long address) {
        return Double.longBitsToDouble(getLong(address));
    }

    /**
     * Stores given double value. Alignment aware.
     *
     * @param address Address.
     * @param val Value.
     */
    @Override
    public void putDouble(long address, double val) {
        putLong(address, Double.doubleToRawLongBits(val));
    }

    /**
     * Integer CAS.
     *
     * @param obj Object.
     * @param address Address.
     * @param exp Expected.
     * @param upd Upd.
     * @return {@code True} if operation completed successfully, {@code false} - otherwise.
     */
    @Override
    public boolean compareAndSwapInt(Object obj, long address, int exp, int upd) {
        return compareAndSwapInt(address, exp, upd);
    }

    /**
     * Long CAS.
     *
     * @param obj Object.
     * @param address Address.
     * @param exp Expected.
     * @param upd Upd.
     * @return {@code True} if operation completed successfully, {@code false} - otherwise.
     */
    @Override
    public boolean compareAndSwapLong(Object obj, long address, long exp, long upd) {
        return compareAndSwapLong(address, exp, upd);
    }

    /**
     * Integer CAS.
     *
     * @param address
     * @param exp
     * @param upd
     * @return
     */
    public boolean compareAndSwapInt(long address, int exp, int upd) {
        casLock.lock();
        try {
            if (getInt(address) == exp) {
                putInt(address, upd);
                return true;
            } else {
                return false;
            }
        } finally {
            casLock.unlock();
        }
    }

    /**
     * Long CAS.
     *
     * @param address
     * @param exp
     * @param upd
     * @return
     */
    public boolean compareAndSwapLong(long address, long exp, long upd) {
        casLock.lock();
        try {
            if (getLong(address) == exp) {
                putLong(address, upd);
                return true;
            } else {
                return false;
            }
        } finally {
            casLock.unlock();
        }
    }

    /**
     * Gets byte value with volatile semantic.
     *
     * @param obj Object.
     * @param offset Offset.
     * @return Int value.
     */
    @Override
    public byte getByteVolatile(Object obj, long offset) {
        assert obj == null;
        return getByte(offset);
    }

    /**
     * Stores int value with volatile semantic.
     *
     * @param obj Object.
     * @param offset Offset.
     * @param val Value.
     */
    @Override
    public void putByteVolatile(Object obj, long offset, byte val) {
        assert obj == null;
        putByte(offset, val);
    }

    /**
     * Gets int value with volatile semantic.
     *
     * @param obj Object.
     * @param offset Offset.
     * @return Int value.
     */
    @Override
    public int getIntVolatile(Object obj, long offset) {
        assert obj == null;
        return getInt(offset);
    }

    /**
     * Stores int value with volatile semantic.
     *
     * @param obj Object.
     * @param offset Offset.
     * @param val Value.
     */
    @Override
    public void putIntVolatile(Object obj, long offset, int val) {
        assert obj == null;
        putInt(offset, val);
    }

    /**
     * Gets long value with volatile semantic.
     *
     * @param obj Object.
     * @param offset Offset.
     * @return Long value.
     */
    @Override
    public long getLongVolatile(Object obj, long offset) {
        assert obj == null;
        return getLong(offset);
    }

    /**
     * Stores long value with volatile semantic.
     *
     * @param obj Object.
     * @param offset Offset.
     * @param val Value.
     */
    @Override
    public void putLongVolatile(Object obj, long offset, long val) {
        assert obj == null;
        putLong(offset, val);
    }

}
