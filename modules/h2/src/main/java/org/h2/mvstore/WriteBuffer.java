/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;

/**
 * An auto-resize buffer to write data into a ByteBuffer.
 */
public class WriteBuffer {

    /**
     * The maximum size of the buffer in order to be re-used after a clear
     * operation.
     */
    private static final int MAX_REUSE_CAPACITY = 4 * 1024 * 1024;

    /**
     * The minimum number of bytes to grow a buffer at a time.
     */
    private static final int MIN_GROW = 1024 * 1024;

    /**
     * The buffer that is used after a clear operation.
     */
    private ByteBuffer reuse;

    /**
     * The current buffer (may be replaced if it is too small).
     */
    private ByteBuffer buff;

    public WriteBuffer(int initialSize) {
        reuse = ByteBuffer.allocate(initialSize);
        buff = reuse;
    }

    public WriteBuffer() {
        this(MIN_GROW);
    }

    /**
     * Write a variable size integer.
     *
     * @param x the value
     * @return this
     */
    public WriteBuffer putVarInt(int x) {
        DataUtils.writeVarInt(ensureCapacity(5), x);
        return this;
    }

    /**
     * Write a variable size long.
     *
     * @param x the value
     * @return this
     */
    public WriteBuffer putVarLong(long x) {
        DataUtils.writeVarLong(ensureCapacity(10), x);
        return this;
    }

    /**
     * Write the characters of a string in a format similar to UTF-8.
     *
     * @param s the string
     * @param len the number of characters to write
     * @return this
     */
    public WriteBuffer putStringData(String s, int len) {
        ByteBuffer b = ensureCapacity(3 * len);
        DataUtils.writeStringData(b, s, len);
        return this;
    }

    /**
     * Put a byte.
     *
     * @param x the value
     * @return this
     */
    public WriteBuffer put(byte x) {
        ensureCapacity(1).put(x);
        return this;
    }

    /**
     * Put a character.
     *
     * @param x the value
     * @return this
     */
    public WriteBuffer putChar(char x) {
        ensureCapacity(2).putChar(x);
        return this;
    }

    /**
     * Put a short.
     *
     * @param x the value
     * @return this
     */
    public WriteBuffer putShort(short x) {
        ensureCapacity(2).putShort(x);
        return this;
    }

    /**
     * Put an integer.
     *
     * @param x the value
     * @return this
     */
    public WriteBuffer putInt(int x) {
        ensureCapacity(4).putInt(x);
        return this;
    }

    /**
     * Put a long.
     *
     * @param x the value
     * @return this
     */
    public WriteBuffer putLong(long x) {
        ensureCapacity(8).putLong(x);
        return this;
    }

    /**
     * Put a float.
     *
     * @param x the value
     * @return this
     */
    public WriteBuffer putFloat(float x) {
        ensureCapacity(4).putFloat(x);
        return this;
    }

    /**
     * Put a double.
     *
     * @param x the value
     * @return this
     */
    public WriteBuffer putDouble(double x) {
        ensureCapacity(8).putDouble(x);
        return this;
    }

    /**
     * Put a byte array.
     *
     * @param bytes the value
     * @return this
     */
    public WriteBuffer put(byte[] bytes) {
        ensureCapacity(bytes.length).put(bytes);
        return this;
    }

    /**
     * Put a byte array.
     *
     * @param bytes the value
     * @param offset the source offset
     * @param length the number of bytes
     * @return this
     */
    public WriteBuffer put(byte[] bytes, int offset, int length) {
        ensureCapacity(length).put(bytes, offset, length);
        return this;
    }

    /**
     * Put the contents of a byte buffer.
     *
     * @param src the source buffer
     * @return this
     */
    public WriteBuffer put(ByteBuffer src) {
        ensureCapacity(src.remaining()).put(src);
        return this;
    }

    /**
     * Set the limit, possibly growing the buffer.
     *
     * @param newLimit the new limit
     * @return this
     */
    public WriteBuffer limit(int newLimit) {
        ensureCapacity(newLimit - buff.position()).limit(newLimit);
        return this;
    }

    /**
     * Get the capacity.
     *
     * @return the capacity
     */
    public int capacity() {
        return buff.capacity();
    }

    /**
     * Set the position.
     *
     * @param newPosition the new position
     * @return the new position
     */
    public WriteBuffer position(int newPosition) {
        buff.position(newPosition);
        return this;
    }

    /**
     * Get the limit.
     *
     * @return the limit
     */
    public int limit() {
        return buff.limit();
    }

    /**
     * Get the current position.
     *
     * @return the position
     */
    public int position() {
        return buff.position();
    }

    /**
     * Copy the data into the destination array.
     *
     * @param dst the destination array
     * @return this
     */
    public WriteBuffer get(byte[] dst) {
        buff.get(dst);
        return this;
    }

    /**
     * Update an integer at the given index.
     *
     * @param index the index
     * @param value the value
     * @return this
     */
    public WriteBuffer putInt(int index, int value) {
        buff.putInt(index, value);
        return this;
    }

    /**
     * Update a short at the given index.
     *
     * @param index the index
     * @param value the value
     * @return this
     */
    public WriteBuffer putShort(int index, short value) {
        buff.putShort(index, value);
        return this;
    }

    /**
     * Clear the buffer after use.
     *
     * @return this
     */
    public WriteBuffer clear() {
        if (buff.limit() > MAX_REUSE_CAPACITY) {
            buff = reuse;
        } else if (buff != reuse) {
            reuse = buff;
        }
        buff.clear();
        return this;
    }

    /**
     * Get the byte buffer.
     *
     * @return the byte buffer
     */
    public ByteBuffer getBuffer() {
        return buff;
    }

    private ByteBuffer ensureCapacity(int len) {
        if (buff.remaining() < len) {
            grow(len);
        }
        return buff;
    }

    private void grow(int additional) {
        ByteBuffer temp = buff;
        int needed = additional - temp.remaining();
        // grow at least MIN_GROW
        long grow = Math.max(needed, MIN_GROW);
        // grow at least 50% of the current size
        grow = Math.max(temp.capacity() / 2, grow);
        // the new capacity is at most Integer.MAX_VALUE
        int newCapacity = (int) Math.min(Integer.MAX_VALUE, temp.capacity() + grow);
        if (newCapacity < needed) {
            throw new OutOfMemoryError("Capacity: " + newCapacity + " needed: " + needed);
        }
        try {
            buff = ByteBuffer.allocate(newCapacity);
        } catch (OutOfMemoryError e) {
            throw new OutOfMemoryError("Capacity: " + newCapacity);
        }
        temp.flip();
        buff.put(temp);
        if (newCapacity <= MAX_REUSE_CAPACITY) {
            reuse = buff;
        }
    }

}
