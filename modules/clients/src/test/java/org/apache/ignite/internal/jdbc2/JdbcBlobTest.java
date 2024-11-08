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

package org.apache.ignite.internal.jdbc2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.Arrays;
import org.junit.Test;

import static org.apache.ignite.internal.binary.streams.BinaryAbstractOutputStream.MAX_ARRAY_SIZE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** */
public class JdbcBlobTest {
    /** */
    static final String ERROR_BLOB_FREE = "Blob instance can't be used after free() has been called.";

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLength() throws Exception {
        JdbcBlob blob = new JdbcBlob(new byte[16]);

        assertEquals(16, (int)blob.length());

        blob.free();

        try {
            blob.length();

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetBytes() throws Exception {
        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

        JdbcBlob blob = new JdbcBlob(arr);

        try {
            blob.getBytes(0, 16);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            blob.getBytes(17, 16);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            blob.getBytes(1, -1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        byte[] res = blob.getBytes(1, 0);
        assertEquals(0, res.length);

        assertTrue(Arrays.equals(arr, blob.getBytes(1, 16)));

        res = blob.getBytes(1, 20);
        assertEquals(16, res.length);
        assertTrue(Arrays.equals(arr, res));

        res = blob.getBytes(1, 10);
        assertEquals(10, res.length);
        assertEquals(0, res[0]);
        assertEquals(9, res[9]);

        res = blob.getBytes(7, 10);
        assertEquals(10, res.length);
        assertEquals(6, res[0]);
        assertEquals(15, res[9]);

        res = blob.getBytes(7, 20);
        assertEquals(10, res.length);
        assertEquals(6, res[0]);
        assertEquals(15, res[9]);

        res = blob.getBytes(1, 0);
        assertEquals(0, res.length);

        blob = new JdbcBlob(new byte[0]);
        assertEquals(0, blob.getBytes(1, 0).length);

        blob.free();

        try {
            blob.getBytes(1, 16);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetBinaryStream() throws Exception {
        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

        JdbcBlob blob = new JdbcBlob(arr);

        InputStream is = blob.getBinaryStream();
        byte[] res = readBytes(is);
        assertTrue(Arrays.equals(arr, res));

        blob.free();

        try {
            blob.getBinaryStream();

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetBinaryStreamReadMoreThenBlobSize() throws Exception {
        byte[] arr = new byte[] {1, 2, 3, 4, 5};

        JdbcBlob blob = new JdbcBlob(arr);

        InputStream is = blob.getBinaryStream();
        byte[] res = new byte[7];
        assertEquals(5, is.read(res, 0, 7));
        assertArrayEquals(new byte[] {1, 2, 3, 4, 5, 0, 0}, res);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetBinaryStreamWithParams() throws Exception {
        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

        JdbcBlob blob = new JdbcBlob(arr);

        try {
            blob.getBinaryStream(0, arr.length);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            blob.getBinaryStream(1, 0);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            blob.getBinaryStream(17, arr.length);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            blob.getBinaryStream(1, arr.length + 1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        InputStream is = blob.getBinaryStream(1, arr.length);
        byte[] res = readBytes(is);
        assertTrue(Arrays.equals(arr, res));

        is = blob.getBinaryStream(1, 10);
        res = readBytes(is);
        assertEquals(10, res.length);
        assertEquals(0, res[0]);
        assertEquals(9, res[9]);

        is = blob.getBinaryStream(6, 10);
        res = readBytes(is);
        assertEquals(10, res.length);
        assertEquals(5, res[0]);
        assertEquals(14, res[9]);

        blob.free();

        try {
            blob.getBinaryStream(1, arr.length);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetBinaryStreamWithParamsReadMoreThenStreamLimit() throws Exception {
        byte[] arr = new byte[] {1, 2, 3, 4, 5};

        JdbcBlob blob = new JdbcBlob(arr);

        InputStream is = blob.getBinaryStream(2, 3);
        byte[] res = new byte[6];
        assertEquals(3, is.read(res, 1, 5));
        assertArrayEquals(new byte[] {0, 2, 3, 4, 0, 0}, res);
        assertEquals(-1, is.read(res, 0, 1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetBinaryStreamReadFromTruncated() throws Exception {
        byte[] arr = new byte[]{0, 1, 2, 3, 4, 5, 6, 7};

        JdbcBlob blob = new JdbcBlob(JdbcBinaryBuffer.createReadOnly(arr, 0, arr.length));

        InputStream is = blob.getBinaryStream(2, 4);

        assertEquals(1, is.read());

        blob.truncate(6);

        assertEquals(2, is.read());

        blob.truncate(2);

        assertEquals(-1, is.read());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetBinaryStreamMarkSkipReset() throws Exception {
        byte[] arr = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8};

        JdbcBlob blob = new JdbcBlob(JdbcBinaryBuffer.createReadWrite(arr));

        InputStream is = blob.getBinaryStream(2, arr.length - 2);

        assertEquals(1, is.read());
        is.reset();
        assertEquals(1, is.read());

        assertEquals(2, is.read());
        is.mark(1);
        assertEquals(3, is.read());
        assertEquals(4, is.read());
        is.reset();
        assertEquals(3, is.read());

        assertEquals(0, is.skip(-1));
        assertEquals(0, is.skip(0));
        assertEquals(1, is.skip(1));
        assertEquals(2, is.skip(2));
        assertEquals(7, is.read());

        is.reset();
        assertEquals(3, is.read());

        assertEquals(4, is.skip(100));
        assertEquals(-1, is.read());

        is.reset();
        assertEquals(5, is.skip(Long.MAX_VALUE));
        assertEquals(-1, is.read());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetBinaryStreamSeeChangesDoneAfterCreate() throws Exception {
        byte[] arr = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8};

        JdbcBlob blob = new JdbcBlob(JdbcBinaryBuffer.createReadWrite(arr));

        InputStream is = blob.getBinaryStream();

        assertEquals(0, is.read());
        assertEquals(1, is.read());

        OutputStream os = blob.setBinaryStream(3);
        os.write(11);

        assertEquals(11, is.read());

        blob.setBytes(4, new byte[] {12});

        assertEquals(12, is.read());

        byte[] res = is.readAllBytes();
        assertArrayEquals(new byte[] {4, 5, 6, 7, 8}, res);

        blob.setBytes(blob.length() + 1, new byte[] {13, 14, 15});
        assertEquals(13, is.read());
        assertEquals(14, is.read());
        assertEquals(15, is.read());
        assertEquals(-1, is.read());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetBinaryStreamWithParamsSeeChangesDoneAfterCreate() throws Exception {
        byte[] arr = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8};

        JdbcBlob blob = new JdbcBlob(JdbcBinaryBuffer.createReadOnly(arr, 0, arr.length));

        InputStream is = blob.getBinaryStream(2, arr.length - 2);

        assertEquals(1, is.read());

        OutputStream os = blob.setBinaryStream(3);
        os.write(11);

        assertEquals(11, is.read());

        blob.setBytes(4, new byte[] {12});

        assertEquals(12, is.read());

        byte[] res = is.readNBytes(4);
        assertArrayEquals(new byte[] {4, 5, 6, 7}, res);

        blob.setBytes(blob.length() + 1, new byte[] {13});
        assertEquals(-1, is.read());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPositionBytePattern() throws Exception {
        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

        JdbcBlob blob = new JdbcBlob(arr);

        assertThrows(null, () -> blob.position(new byte[] {1, 2, 3}, 0), SQLException.class, null);
        assertEquals(-1, blob.position(new byte[] {1, 2, 3}, arr.length + 1));
        assertEquals(-1, blob.position(new byte[0], 1));
        assertEquals(-1, blob.position(new byte[17], 1));
        assertEquals(-1, blob.position(new byte[] {3, 2, 1}, 1));
        assertEquals(1, blob.position(new byte[] {0, 1, 2}, 1));
        assertEquals(2, blob.position(new byte[] {1, 2, 3}, 1));
        assertEquals(2, blob.position(new byte[] {1, 2, 3}, 2));
        assertEquals(-1, blob.position(new byte[] {1, 2, 3}, 3));
        assertEquals(14, blob.position(new byte[] {13, 14, 15}, 3));
        assertEquals(-1, blob.position(new byte[] {0, 1, 3}, 1));
        assertEquals(-1, blob.position(new byte[] {0, 2, 3}, 1));
        assertEquals(-1, blob.position(new byte[] {1, 2, 4}, 1));

        blob.setBytes(17, new byte[] {16, 16, 16, 33, 46});
        assertEquals(18, blob.position(new byte[] {16, 16, 33}, 1));

        blob.free();

        try {
            blob.position(new byte[] {0, 1, 2}, 1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPositionBlobPattern() throws Exception {
        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

        JdbcBlob blob = new JdbcBlob(arr);

        assertThrows(null, () -> blob.position(new JdbcBlob(new byte[] {1, 2, 3}), 0), SQLException.class, null);
        assertEquals(-1, blob.position(new JdbcBlob(new byte[] {1, 2, 3}), arr.length + 1));
        assertEquals(-1, blob.position(new JdbcBlob(new byte[0]), 1));
        assertEquals(-1, blob.position(new JdbcBlob(new byte[17]), 1));
        assertEquals(-1, blob.position(new JdbcBlob(new byte[] {3, 2, 1}), 1));
        assertEquals(1, blob.position(new JdbcBlob(new byte[] {0, 1, 2}), 1));
        assertEquals(2, blob.position(new JdbcBlob(new byte[] {1, 2, 3}), 1));
        assertEquals(2, blob.position(new JdbcBlob(new byte[] {1, 2, 3}), 2));
        assertEquals(-1, blob.position(new JdbcBlob(new byte[] {1, 2, 3}), 3));
        assertEquals(14, blob.position(new JdbcBlob(new byte[] {13, 14, 15}), 3));
        assertEquals(-1, blob.position(new JdbcBlob(new byte[] {0, 1, 3}), 1));
        assertEquals(-1, blob.position(new JdbcBlob(new byte[] {0, 2, 3}), 1));
        assertEquals(-1, blob.position(new JdbcBlob(new byte[] {1, 2, 4}), 1));

        blob.setBytes(17, new byte[] {16, 16, 16, 33, 46});
        assertEquals(18, blob.position(new JdbcBlob(new byte[] {16, 16, 33}), 1));

        blob.free();

        try {
            blob.position(new JdbcBlob(new byte[] {0, 1, 2}), 1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetBytes() throws Exception {
        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7};

        JdbcBlob blob = new JdbcBlob(arr);

        try {
            blob.setBytes(0, new byte[4]);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            blob.setBytes(17, new byte[4]);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        assertEquals(4, blob.setBytes(1, new byte[] {3, 2, 1, 0}));
        assertTrue(Arrays.equals(new byte[] {3, 2, 1, 0, 4, 5, 6, 7}, blob.getBytes(1, arr.length)));

        assertEquals(4, blob.setBytes(5, new byte[] {7, 6, 5, 4}));
        assertTrue(Arrays.equals(new byte[] {3, 2, 1, 0, 7, 6, 5, 4}, blob.getBytes(1, arr.length)));

        assertEquals(4, blob.setBytes(7, new byte[] {8, 9, 10, 11}));
        assertTrue(Arrays.equals(new byte[] {3, 2, 1, 0, 7, 6, 8, 9, 10, 11}, blob.getBytes(1, (int)blob.length())));

        blob = new JdbcBlob(new byte[] {15, 16});
        assertEquals(8, blob.setBytes(1, new byte[] {0, 1, 2, 3, 4, 5, 6, 7}));
        assertTrue(Arrays.equals(new byte[] {0, 1, 2, 3, 4, 5, 6, 7}, blob.getBytes(1, (int)blob.length())));

        blob.free();

        try {
            blob.setBytes(1, new byte[] {0, 1, 2});

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetBytesRO() throws Exception {
        byte[] roArr = new byte[]{0, 1, 2, 3, 4, 5, 6, 7};

        Blob blob = new JdbcBlob(JdbcBinaryBuffer.createReadOnly(roArr, 2, 4));

        blob.setBytes(2, new byte[] {11, 22});

        assertArrayEquals(new byte[] {2, 11, 22, 5}, blob.getBytes(1, (int)blob.length()));

        assertArrayEquals(new byte[] {0, 1, 2, 3, 4, 5, 6, 7}, roArr);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetBytesRealloc() throws Exception {
        byte[] roArr = new byte[]{0, 1, 2, 3, 4, 5, 6, 7};

        Blob blob = new JdbcBlob(JdbcBinaryBuffer.createReadOnly(roArr, 2, 4));

        blob.setBytes(5, new byte[JdbcBinaryBuffer.MIN_CAP]);

        assertEquals(JdbcBinaryBuffer.MIN_CAP + 4, blob.length());

        assertArrayEquals(new byte[] {0, 1, 2, 3, 4, 5, 6, 7}, roArr);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetBytesTooMuchData() throws Exception {
        Blob blob = new JdbcBlob();

        blob.setBytes(1, new byte[1]);

        // Use fake one byte array.
        assertThrows(null, () -> blob.setBytes(2, new byte[1], 0, MAX_ARRAY_SIZE),
                SQLException.class, "Too much data. Can't write more then 2147483639 bytes.");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetBytesWithOffsetAndLength() throws Exception {
        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7};

        JdbcBlob blob = new JdbcBlob(arr);

        try {
            blob.setBytes(0, new byte[4], 0, 2);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            blob.setBytes(17, new byte[4], 0, 2);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            blob.setBytes(1, new byte[4], -1, 2);

            fail();
        }
        catch (IndexOutOfBoundsException e) {
            // No-op.
        }

        try {
            blob.setBytes(1, new byte[4], 0, 5);

            fail();
        }
        catch (IndexOutOfBoundsException e) {
            // No-op.
        }

        assertEquals(4, blob.setBytes(1, new byte[] {3, 2, 1, 0}, 0, 4));
        assertTrue(Arrays.equals(new byte[] {3, 2, 1, 0, 4, 5, 6, 7}, blob.getBytes(1, arr.length)));

        assertEquals(4, blob.setBytes(5, new byte[] {7, 6, 5, 4}, 0, 4));
        assertTrue(Arrays.equals(new byte[] {3, 2, 1, 0, 7, 6, 5, 4}, blob.getBytes(1, arr.length)));

        assertEquals(4, blob.setBytes(7, new byte[] {8, 9, 10, 11}, 0, 4));
        assertTrue(Arrays.equals(new byte[] {3, 2, 1, 0, 7, 6, 8, 9, 10, 11}, blob.getBytes(1, (int)blob.length())));

        assertEquals(2, blob.setBytes(1, new byte[] {3, 2, 1, 0}, 2, 2));
        assertTrue(Arrays.equals(new byte[] {1, 0, 1, 0, 7, 6, 8, 9, 10, 11}, blob.getBytes(1, (int)blob.length())));

        assertEquals(2, blob.setBytes(9, new byte[] {3, 2, 1, 0}, 1, 2));
        assertTrue(Arrays.equals(new byte[] {1, 0, 1, 0, 7, 6, 8, 9, 2, 1}, blob.getBytes(1, (int)blob.length())));

        assertEquals(3, blob.setBytes(9, new byte[] {3, 2, 1, 0}, 0, 3));
        assertTrue(Arrays.equals(new byte[] {1, 0, 1, 0, 7, 6, 8, 9, 3, 2, 1}, blob.getBytes(1, (int)blob.length())));

        blob = new JdbcBlob(new byte[] {15, 16});
        assertEquals(8, blob.setBytes(1, new byte[] {0, 1, 2, 3, 4, 5, 6, 7}, 0, 8));
        assertTrue(Arrays.equals(new byte[] {0, 1, 2, 3, 4, 5, 6, 7}, blob.getBytes(1, (int)blob.length())));

        blob.free();

        try {
            blob.setBytes(1, new byte[] {0, 1, 2}, 0, 2);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTruncate() throws Exception {
        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7};

        JdbcBlob blob = new JdbcBlob(arr);

        try {
            blob.truncate(-1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        try {
            blob.truncate(arr.length + 1);

            fail();
        }
        catch (SQLException e) {
            // No-op.
        }

        blob.truncate(4);
        assertTrue(Arrays.equals(new byte[] {0, 1, 2, 3}, blob.getBytes(1, (int)blob.length())));

        blob.truncate(0);
        assertEquals(0, (int)blob.length());

        blob.free();

        try {
            blob.truncate(0);

            fail();
        }
        catch (SQLException e) {
            // No-op.
            System.out.println();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTruncateRO() throws Exception {
        byte[] roArr = new byte[]{0, 1, 2, 3, 4, 5, 6, 7};

        Blob blob = new JdbcBlob(JdbcBinaryBuffer.createReadOnly(roArr, 2, 4));
        assertArrayEquals(new byte[] {2, 3, 4, 5}, blob.getBytes(1, (int)blob.length()));

        blob.truncate(2);
        assertArrayEquals(new byte[] {2, 3}, blob.getBytes(1, (int)blob.length()));

        assertArrayEquals(new byte[] {0, 1, 2, 3, 4, 5, 6, 7}, roArr);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFree() throws Exception {
        Blob blob = new JdbcBlob(new byte[] {0, 1, 2, 3, 4, 5, 6, 7});

        assertEquals(8, blob.length());

        blob.free();

        blob.free();

        assertThrows(null, blob::length, SQLException.class, ERROR_BLOB_FREE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetBinaryStream() throws Exception {
        Blob blob = new JdbcBlob();

        assertThrows(null, () -> blob.setBinaryStream(0), SQLException.class, null);
        assertThrows(null, () -> blob.setBinaryStream(2), SQLException.class, null);

        OutputStream os = blob.setBinaryStream(1);

        os.write(0);
        os.write(new byte[] {1, 2, 3, 4, 5, 6, 7});
        os.close();
        assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7}, blob.getBytes(1, (int)blob.length()));

        os = blob.setBinaryStream(3);
        os.write(new byte[] {20, 21, 22});
        os.write(23);
        os.close();
        assertArrayEquals(new byte[]{0, 1, 20, 21, 22, 23, 6, 7}, blob.getBytes(1, (int)blob.length()));

        os = blob.setBinaryStream(7);
        os.write(new byte[] {30, 31, 32});
        os.write(33);
        os.close();
        assertArrayEquals(new byte[]{0, 1, 20, 21, 22, 23, 30, 31, 32, 33}, blob.getBytes(1, (int)blob.length()));

        blob.free();
        assertThrows(null, () -> blob.setBinaryStream(2L), SQLException.class, ERROR_BLOB_FREE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetBinaryStreamTooMuchData() throws Exception {
        Blob blob = new JdbcBlob();

        OutputStream os = blob.setBinaryStream(1);
        os.write(1);

        assertThrows(null, () -> {
            // Use fake one byte array.
            os.write(new byte[1], 0, MAX_ARRAY_SIZE);

            return null;
        }, IOException.class, "Too much data. Can't write more then 2147483639 bytes.");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetBinaryStreamRO() throws Exception {
        byte[] roArr = new byte[]{0, 1, 2, 3, 4, 5, 6, 7};

        Blob blob = new JdbcBlob(JdbcBinaryBuffer.createReadOnly(roArr, 2, 4));
        assertArrayEquals(new byte[] {2, 3, 4, 5}, blob.getBytes(1, (int)blob.length()));

        OutputStream os = blob.setBinaryStream(5);
        os.write(new byte[] {11, 22});
        os.close();
        assertArrayEquals(new byte[] {2, 3, 4, 5, 11, 22}, blob.getBytes(1, (int)blob.length()));

        assertArrayEquals(new byte[] {0, 1, 2, 3, 4, 5, 6, 7}, roArr);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetBinaryStreamWriteToTruncated() throws Exception {
        Blob blob = new JdbcBlob();

        OutputStream os = blob.setBinaryStream(1);

        os.write(1);

        blob.truncate(0);

        assertThrows(null, () -> {
            os.write(2);

            return null;
        }, IOException.class, "Writting beyond end of Blob, it probably was truncated after OutputStream " +
                "was created [pos=1, blobLength=0]");

        assertThrows(null, () -> {
            os.write(new byte[] {2});

            return null;
        }, IOException.class, "Writting beyond end of Blob, it probably was truncated after OutputStream " +
                "was created [pos=1, blobLength=0]");

        os.close();
    }

    /**
     * @param is Input stream.
     */
    private static byte[] readBytes(InputStream is) throws IOException {
        byte[] tmp = new byte[16];

        int i = 0;
        int read;
        int cnt = 0;

        while ((read = is.read()) != -1) {
            tmp[i++] = (byte)read;
            cnt++;
        }

        byte[] res = new byte[cnt];

        System.arraycopy(tmp, 0, res, 0, cnt);

        return res;
    }
}
