/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * Tests bit output stream.
 */
public class GridBitStreamsSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testSingleBits() throws Exception {
        GridBitByteArrayOutputStream out = new GridBitByteArrayOutputStream();

        Random rnd = new Random();

        byte[] testData = new byte[8192];

        rnd.nextBytes(testData);

        for (byte aTestData : testData) {
            for (int b = 0; b < 8; b++)
                out.writeBit(((aTestData >>> b) & 0x01) == 1);
        }

        assertEquals(testData.length, out.bytesLength());
        assertEquals(testData.length * 8, out.bitsLength());

        byte[] bytes = out.toArray();

        assertEquals(testData.length, bytes.length);

        for (int i = 0; i < testData.length; i++)
            assertEquals("Mismatch in position: " + i, testData[i], bytes[i]);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncompleteBytes() throws Exception {
        GridBitByteArrayOutputStream out = new GridBitByteArrayOutputStream();

        assertEquals(0, out.bytesLength());
        assertEquals(0, out.bitsLength());

        out.writeBit(true);
        out.writeBit(true);
        out.writeBit(true);
        out.writeBit(true);

        assertEquals(1, out.bytesLength());
        assertEquals(4, out.bitsLength());

        out.writeBit(false);
        out.writeBit(false);
        out.writeBit(false);

        assertEquals(1, out.bytesLength());
        assertEquals(7, out.bitsLength());

        out.writeBit(true);

        assertEquals(1, out.bytesLength());
        assertEquals(8, out.bitsLength());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartialBits() throws Exception {
        GridBitByteArrayOutputStream out = new GridBitByteArrayOutputStream();

        for (int i = 0; i < 10; i++) {
            for (int b = 0; b < 8; b++)
                out.writeBits(0b101, 3);
        }

        byte[] bytes = out.toArray();

        assertEquals(30, bytes.length);

        for (int i = 0; i < 10; i++) {
            assertEquals(0b01101101, bytes[i * 3] & 0xFF);
            assertEquals(0b11011011, bytes[i * 3 + 1] & 0xFF);
            assertEquals(0b10110110, bytes[i * 3 + 2] & 0xFF);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartialBits2() throws Exception {
        GridBitByteArrayOutputStream out = new GridBitByteArrayOutputStream();

        for (int i = 0; i < 10; i++) {
            for (int b = 0; b < 8; b++)
                out.writeBits(0b111000111, 9);
        }

        byte[] bytes = out.toArray();

        assertEquals(90, bytes.length);

        for (int i = 0; i < 10; i++) {
            assertEquals(0b11000111, bytes[i * 9] & 0xFF);
            assertEquals(0b10001111, bytes[i * 9 + 1] & 0xFF);
            assertEquals(0b00011111, bytes[i * 9 + 2] & 0xFF);
            assertEquals(0b00111111, bytes[i * 9 + 3] & 0xFF);
            assertEquals(0b01111110, bytes[i * 9 + 4] & 0xFF);
            assertEquals(0b11111100, bytes[i * 9 + 5] & 0xFF);
            assertEquals(0b11111000, bytes[i * 9 + 6] & 0xFF);
            assertEquals(0b11110001, bytes[i * 9 + 7] & 0xFF);
            assertEquals(0b11100011, bytes[i * 9 + 8] & 0xFF);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartialBits3() throws Exception {
        GridBitByteArrayOutputStream out = new GridBitByteArrayOutputStream();

        for (int i = 0; i < 10; i++) {
            for (int b = 0; b < 4; b++)
                out.writeBits(0b111100001111000011, 18);
        }

        byte[] bytes = out.toArray();

        assertEquals(90, bytes.length);

        for (int i = 0; i < 10; i++) {
            assertEquals(0b11000011, bytes[i * 9] & 0xFF);
            assertEquals(0b11000011, bytes[i * 9 + 1] & 0xFF);
            assertEquals(0b00001111, bytes[i * 9 + 2] & 0xFF);
            assertEquals(0b00001111, bytes[i * 9 + 3] & 0xFF);
            assertEquals(0b00111111, bytes[i * 9 + 4] & 0xFF);
            assertEquals(0b00111100, bytes[i * 9 + 5] & 0xFF);
            assertEquals(0b11111100, bytes[i * 9 + 6] & 0xFF);
            assertEquals(0b11110000, bytes[i * 9 + 7] & 0xFF);
            assertEquals(0b11110000, bytes[i * 9 + 8] & 0xFF);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadSingleBits() throws Exception {
        for (int len = 1; len <= 48; len++) {
            info("Checking length: " + len);

            byte[] data = new byte[] {0b01010101, 0b01010101, 0b01010101, 0b01010101, 0b01010101, 0b01010101};

            GridBitByteArrayInputStream in = new GridBitByteArrayInputStream(data, len);

            boolean expected = true;

            for (int i = 0; i < len; i++) {
                assertEquals("Failed in position: " + i, len - i, in.available());

                assertEquals("Failed in position: " + i, expected, in.readBit());

                expected = !expected;
            }

            try {
                in.readBit();

                fail("Should fail with EOF");
            }
            catch (EOFException ignored) {
                // Expected.
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadMultipleBits1() throws Exception {
        byte[] data = new byte[] {0b01101101, (byte)0b11011011, (byte)0b10110110};

        GridBitByteArrayInputStream in = new GridBitByteArrayInputStream(data, data.length * 8);

        for (int i = 0; i < 8; i++)
            assertEquals("Failed in position: " + i, 0b101, in.readBits(3));

        try {
            in.readBit();

            fail("Should fail with EOF");
        }
        catch (EOFException ignored) {
            // Expected.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadMultipleBits2() throws Exception {
        byte[] data = new byte[] {(byte)0b11000111, (byte)0b10001111,  0b00011111, 0b00111111,  0b01111110, (byte)0b11111100, (byte)0b11111000, (byte)0b11110001, (byte)0b11100011};

        GridBitByteArrayInputStream in = new GridBitByteArrayInputStream(data, data.length * 8);

        for (int i = 0; i < 8; i++) {
            info("Checking: " + i);
            assertEquals("Failed in position: " + i, 0b111000111, in.readBits(9));
        }

        try {
            in.readBit();

            fail("Should fail with EOF");
        }
        catch (EOFException ignored) {
            // Expected.
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadWrite() throws Exception {
        int num = 1024;
        Random rnd = new Random();

        for (int r = 0; r < 10; r++) {
            Collection<GridBiTuple<Integer, Integer>> written = new ArrayList<>(num);

            GridBitByteArrayOutputStream out = new GridBitByteArrayOutputStream();

            int totalBits = 0;

            for (int i = 0; i < num; i++) {
                int bits = rnd.nextInt(31) + 1;

                int mask = (int)((2L << (bits - 1)) - 1);

                int val = rnd.nextInt() & mask;

                assert val == (val & mask);

                written.add(F.t(val, bits));

                info("Written [bits=" + bits + ", val=" + val + ']');

                out.writeBits(val, bits);

                totalBits += bits;
            }

            GridBitByteArrayInputStream in = new GridBitByteArrayInputStream(out.toArray(), totalBits);

            for (GridBiTuple<Integer, Integer> tup : written) {
                Integer val = tup.get1();
                Integer bits = tup.get2();

                info("Read [bits=" + bits + ", val=" + val + ']');

                assertEquals(val, (Integer)in.readBits(bits));
            }
        }
    }
}
