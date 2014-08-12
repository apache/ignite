/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Bit input stream.
 */
public class GridBitByteArrayInputStream {
    /** Bit masks. */
    private static final int[] MASKS;

    /** Data array. */
    private byte[] data;

    /** Number of bytes rounded to the bigger value. */
    private int bytesLength;

    /** */
    private int lastByteLength;

    /** Index in data array. */
    private int dataIdx;

    /** Index in byte. */
    private int byteIdx;

    static {
        MASKS = new int[32];

        for (int i = 0; i < MASKS.length; i++) {
            int prevMask = i == 0 ? 0 : MASKS[i - 1];

            MASKS[i] = prevMask | (1 << i);
        }
    }

    /**
     * @param data Data.
     * @param bitsLength Number of bits to read.
     */
    public GridBitByteArrayInputStream(byte[] data, long bitsLength) {
        A.ensure(bitsLength <= data.length * 8, "bitsLength <= data.length * 8");

        this.data = data;

        bytesLength = (int)((bitsLength + 7) / 8);
        lastByteLength = (int)(bitsLength % 8);
    }

    /**
     * Reads one bit from the input stream.
     *
     * @return Read bit.
     * @throws EOFException If end of stream reached.
     */
    public boolean readBit() throws EOFException {
        checkAvailable(1);

        int bit = (data[dataIdx] >>> byteIdx) & 0x01;

        byteIdx++;

        if (byteIdx == 8) {
            dataIdx++;
            byteIdx = 0;
        }

        return bit != 0;
    }

    /**
     * Reads specified number of bits from the input stream.
     *
     * @param numBits Number of bits to read.
     * @return Read bits.
     * @throws EOFException If end of stream reached.
     */
    public int readBits(int numBits) throws EOFException {
        A.ensure(numBits > 0 && numBits <= 32, "numBits > 0 && numBits <= 32");

        checkAvailable(numBits);

        // Read up to byte boundary.
        int remainder = 8 - byteIdx;

        int read = Math.min(remainder, numBits);

        int val = (data[dataIdx] >>> byteIdx) & MASKS[read - 1];

        numBits -= read;
        byteIdx += read;

        if (byteIdx == 8) {
            byteIdx = 0;
            dataIdx++;
        }

        if (numBits == 0)
            return val;

        assert byteIdx == 0;

        while (numBits >= 8) {
            val |= (data[dataIdx++] & 0xFF) << read;

            read += 8;

            numBits -= 8;
        }

        if (numBits > 0) {
            val |= (data[dataIdx] & MASKS[numBits - 1]) << read;

            byteIdx += numBits;
        }

        return val;
    }

    /**
     * Gets number of bits available to read.
     *
     * @return Number of bits available to read.
     */
    public int available() {
        int available = 0;

        // Do not count first and last bytes.
        int wholeBytes = bytesLength - dataIdx;

        if (byteIdx > 0) {
            wholeBytes--;

            available += (8 - byteIdx);
        }

        if (lastByteLength > 0) {
            wholeBytes--;

            available += lastByteLength;

            if (dataIdx == bytesLength - 1 && byteIdx > 0)
                available -= 8;
        }

        if (wholeBytes >= 0)
            available += 8 * wholeBytes;

        return available;
    }

    /**
     * Checks if stream has specified number of bits available to read.
     *
     * @param numBits Number of bits to check.
     * @throws EOFException If check failed.
     */
    private void checkAvailable(int numBits) throws EOFException {
        if (available() < numBits)
            throw new EOFException();
    }
}
