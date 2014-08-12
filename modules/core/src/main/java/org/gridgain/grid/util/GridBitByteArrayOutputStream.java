/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * TODO: Add class description.
 */
public class GridBitByteArrayOutputStream {
    /** Bit masks. */
    private static final int[] MASKS;

    /** */
    private byte[] data;

    /** Index in data array. */
    private int dataIdx;

    /** Bit index in byte. */
    private int bitIdx;

    static {
        MASKS = new int[32];

        for (int i = 0; i < MASKS.length; i++) {
            int prevMask = i == 0 ? 0 : MASKS[i - 1];

            MASKS[i] = prevMask | (1 << i);
        }
    }

    /**
     *
     */
    public GridBitByteArrayOutputStream() {
        this(1024);
    }

    /**
     * @param size Expected stream size in bytes.
     */
    public GridBitByteArrayOutputStream(int size) {
        data = new byte[size];
    }

    /**
     * Writes given number of bits to the stream.
     *
     * @param val Value to write.
     * @param numBits Number of bits to write.
     */
    public void writeBits(int val, int numBits) {
        A.ensure(numBits > 0 && numBits <= 32, "numBits > 0 && numBits <= 32");

        ensureCapacity(numBits);

        // Trim the value.
        val &= MASKS[numBits - 1];

        // Write bits up to the beginning of new byte.
        int remainder = 8 - bitIdx;

        int write = Math.min(remainder, numBits);

        data[dataIdx] |= (val & MASKS[write - 1]) << bitIdx;

        bitIdx += write;

        if (bitIdx == 8) {
            bitIdx = 0;
            dataIdx++;
        }

        numBits -= write;
        val >>>= write;

        if (numBits == 0)
            return;

        assert bitIdx == 0 : "Invalid alignment: " + bitIdx;

        // Write whole bytes.
        while (numBits >= 8) {
            data[dataIdx++] = (byte)val;

            val >>>= 8;
            numBits -= 8;
        }

        assert val == (val & 0xFF);

        data[dataIdx] = (byte)val;

        bitIdx = numBits;
    }

    /**
     * Writes bit to a bit stream.
     *
     * @param bit Bit value.
     */
    public void writeBit(boolean bit) {
        ensureCapacity(1);

        if (bit)
            data[dataIdx] |= 1 << bitIdx;

        bitIdx++;

        if (bitIdx == 8) {
            bitIdx = 0;
            dataIdx++;
        }
    }

    /**
     * Gets length of this stream in bytes. Size is always rounded to the bigger value.
     *
     * @return Length of the stream in bytes.
     */
    public int bytesLength() {
        return bitIdx == 0 ? dataIdx : dataIdx + 1;
    }

    /**
     * Length of this stream in bits.
     *
     * @return Length of the stream in bits.
     */
    public long bitsLength() {
        return dataIdx * 8L + bitIdx;
    }

    /**
     * Gets byte-array representation of this stream.
     *
     * @return Trimmed copy of internal array.
     */
    public byte[] toArray() {
        return Arrays.copyOf(data, bytesLength());
    }

    /**
     * @return Internal array.
     */
    public byte[] internalArray() {
        return data;
    }

    /**
     * Ensures that data array capacity is enough to fit given number of additional bits.
     *
     * @param numBits Number of bits to write.
     */
    private void ensureCapacity(int numBits) {
        int addedBits = numBits - (8 - bitIdx);

        int addedBytes = (addedBits + 8) / 8;

        if (dataIdx + addedBytes >= data.length) {
            byte[] tmp = new byte[data.length * 2];

            System.arraycopy(data, 0, tmp, 0, data.length);

            data = tmp;
        }
    }
}
