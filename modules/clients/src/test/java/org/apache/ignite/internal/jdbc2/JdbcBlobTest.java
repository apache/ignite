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
import java.sql.SQLException;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** */
public class JdbcBlobTest {
    /** */
    static final String ERROR_BLOB_FREE = "Blob instance can't be used after free() has been called.";

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLength() throws Exception {
        JdbcBlob blob = new JdbcBlob(new byte[8]);

        blob.setBytes(9, new byte[8]);

        assertEquals(16, (int)blob.length());

        blob.free();
        assertThrows(null, blob::length, SQLException.class, ERROR_BLOB_FREE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetBytes() throws Exception {
        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

        JdbcBlob blob = new JdbcBlob();

        assertArrayEquals(new byte[0], blob.getBytes(1, 1));

        blob.setBytes(1, new byte[] {0});
        blob.setBytes(2, new byte[] {1, 2});
        blob.setBytes(4, new byte[] {3, 4, 5, 6});
        blob.setBytes(8, new byte[] {7, 8, 9, 10, 11, 12, 13, 14});
        blob.setBytes(16, new byte[] {15});

        assertThrows(null, () -> blob.getBytes(0, 16), SQLException.class, null);
        assertThrows(null, () -> blob.getBytes(17, 16), SQLException.class, null);
        assertThrows(null, () -> blob.getBytes(1, -1), SQLException.class, null);

        byte[] res = blob.getBytes(1, 0);
        assertEquals(0, res.length);

        assertArrayEquals(arr, blob.getBytes(1, 16));

        res = blob.getBytes(1, 20);
        assertEquals(16, res.length);
        assertArrayEquals(arr, res);

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

        JdbcBlob blob2 = new JdbcBlob(new byte[0]);
        assertEquals(0, blob2.getBytes(1, 0).length);

        blob2.free();
        assertThrows(null, () -> blob2.getBytes(1, 16), SQLException.class, ERROR_BLOB_FREE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetBinaryStream() throws Exception {
        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

        JdbcBlob blob = new JdbcBlob();

        assertArrayEquals(new byte[0], readBytes(blob.getBinaryStream()));

        blob.setBytes(1, new byte[] {0});
        blob.setBytes(2, new byte[] {1, 2});
        blob.setBytes(4, new byte[] {3, 4, 5, 6});
        blob.setBytes(8, new byte[] {7, 8, 9, 10, 11, 12, 13, 14});
        blob.setBytes(16, new byte[] {15});

        InputStream is = blob.getBinaryStream();
        byte[] res = readBytes(is);
        assertArrayEquals(arr, res);

        is = blob.getBinaryStream();
        res = new byte[16];
        assertEquals(16, is.read(res, 0, 100));
        assertArrayEquals(arr, res);

        blob.free();
        assertThrows(null, () -> blob.getBinaryStream(), SQLException.class, ERROR_BLOB_FREE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetBinaryStreamWithParams() throws Exception {
        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

        JdbcBlob blob = new JdbcBlob();

        blob.setBytes(1, new byte[] {0});
        blob.setBytes(2, new byte[] {1, 2});
        blob.setBytes(4, new byte[] {3, 4, 5, 6});
        blob.setBytes(8, new byte[] {7, 8, 9, 10, 11, 12, 13, 14});
        blob.setBytes(16, new byte[] {15});

        assertThrows(null, () -> blob.getBinaryStream(0, arr.length), SQLException.class, null);
        assertThrows(null, () -> blob.getBinaryStream(1, 0), SQLException.class, null);
        assertThrows(null, () -> blob.getBinaryStream(17, arr.length), SQLException.class, null);
        assertThrows(null, () -> blob.getBinaryStream(1, arr.length + 1), SQLException.class, null);

        InputStream is = blob.getBinaryStream(1, arr.length);
        byte[] res = readBytes(is);
        assertArrayEquals(arr, res);

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

        is = blob.getBinaryStream(6, 10);
        res = new byte[20];
        assertEquals(10, is.read(res, 1, 13));
        assertEquals(-1, is.read());
        assertEquals(5, res[1]);
        assertEquals(14, res[10]);

        is = blob.getBinaryStream(6, 10);
        res = new byte[200];
        assertEquals(10, is.read(res, 1, 200));
        assertEquals(-1, is.read());
        assertEquals(5, res[1]);
        assertEquals(14, res[10]);

        blob.free();
        assertThrows(null, () -> blob.getBinaryStream(1, arr.length), SQLException.class, ERROR_BLOB_FREE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPositionBytePattern() throws Exception {
        JdbcBlob blob = new JdbcBlob();

        blob.setBytes(1, new byte[] {0});
        blob.setBytes(2, new byte[] {1, 2});
        blob.setBytes(4, new byte[] {3, 4, 5, 6});
        blob.setBytes(8, new byte[] {7, 8, 9, 10, 11, 12, 13, 14});
        blob.setBytes(16, new byte[] {15});

        assertThrows(null, () -> blob.position(new byte[] {1, 2, 3}, -1), SQLException.class, null);
        assertThrows(null, () -> blob.position(new byte[] {1, 2, 3}, 0), SQLException.class, null);

        assertEquals(-1, blob.position(new byte[] {1, 2, 3}, 16 + 1));
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
        assertThrows(null, () -> blob.position(new byte[] {0, 1, 2}, 1), SQLException.class, ERROR_BLOB_FREE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPositionBlobPattern() throws Exception {
        JdbcBlob blob = new JdbcBlob();

        blob.setBytes(1, new byte[] {0});
        blob.setBytes(2, new byte[] {1, 2});
        blob.setBytes(4, new byte[] {3, 4, 5, 6});
        blob.setBytes(8, new byte[] {7, 8, 9, 10, 11, 12, 13, 14});
        blob.setBytes(16, new byte[] {15});

        assertThrows(null, () -> blob.position(new JdbcBlob(new byte[] {1, 2, 3}), -1), SQLException.class, null);
        assertThrows(null, () -> blob.position(new JdbcBlob(new byte[] {1, 2, 3}), 0), SQLException.class, null);

        assertEquals(-1, blob.position(new JdbcBlob(new byte[] {1, 2, 3}), 16 + 1));
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
        JdbcBlob ptrn = new JdbcBlob();
        ptrn.setBytes(1, new byte[] {16});
        ptrn.setBytes(2, new byte[] {16});
        ptrn.setBytes(3, new byte[] {33});
        assertEquals(18, blob.position(ptrn, 1));

        blob.free();
        assertThrows(null, () -> blob.position(new JdbcBlob(new byte[] {0, 1, 2}), 1), SQLException.class, ERROR_BLOB_FREE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetBytes() throws Exception {
        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7};

        JdbcBlob blob = new JdbcBlob(arr);

        assertThrows(null, () -> blob.setBytes(0, new byte[4]), SQLException.class, null);
        assertThrows(null, () -> blob.setBytes(17, new byte[4]), SQLException.class, null);

        assertEquals(4, blob.setBytes(1, new byte[] {3, 2, 1, 0}));
        assertEquals(8, blob.length());
        assertArrayEquals(new byte[]{3, 2, 1, 0, 4, 5, 6, 7}, blob.getBytes(1, arr.length));

        assertEquals(4, blob.setBytes(5, new byte[] {7, 6, 5, 4}));
        assertEquals(8, blob.length());
        assertArrayEquals(new byte[]{3, 2, 1, 0, 7, 6, 5, 4}, blob.getBytes(1, arr.length));

        assertEquals(4, blob.setBytes(7, new byte[] {8, 9, 10, 11}));
        assertEquals(10, blob.length());
        assertArrayEquals(new byte[] {3, 2, 1, 0, 7, 6, 8, 9, 10, 11}, blob.getBytes(1, (int)blob.length()));

        JdbcBlob blob2 = new JdbcBlob(new byte[] {15, 16});
        assertEquals(8, blob2.setBytes(1, new byte[] {0, 1, 2, 3, 4, 5, 6, 7}));
        assertArrayEquals(new byte[] {0, 1, 2, 3, 4, 5, 6, 7}, blob2.getBytes(1, (int)blob2.length()));

        blob2.free();
        assertThrows(null, () -> blob2.setBytes(1, new byte[] {0, 1, 2}), SQLException.class, ERROR_BLOB_FREE);

        JdbcBlob blob3 = new JdbcBlob();
        assertEquals(32 * 1024 * 1024 + 1, blob3.setBytes(1, new byte[32 * 1024 * 1024 + 1]));
        assertEquals(1, blob3.setBytes(32 * 1024 * 1024 + 2, new byte[] {1}));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSetBytesWithOffsetAndLength() throws Exception {
        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7};

        JdbcBlob blob = new JdbcBlob(arr);

        assertThrows(null, () -> blob.setBytes(0, new byte[4], 0, 2), SQLException.class, null);
        assertThrows(null, () -> blob.setBytes(17, new byte[4], 0, 2), SQLException.class, null);
        assertThrowsWithCause(() -> blob.setBytes(1, new byte[4], -1, 2), ArrayIndexOutOfBoundsException.class);
        assertThrowsWithCause(() -> blob.setBytes(1, new byte[4], 0, 5), ArrayIndexOutOfBoundsException.class);
        assertThrowsWithCause(() -> blob.setBytes(1, new byte[4], 4, 5), ArrayIndexOutOfBoundsException.class);

        assertEquals(4, blob.setBytes(1, new byte[] {3, 2, 1, 0}, 0, 4));
        assertArrayEquals(new byte[]{3, 2, 1, 0, 4, 5, 6, 7}, blob.getBytes(1, arr.length));

        assertEquals(4, blob.setBytes(5, new byte[] {7, 6, 5, 4}, 0, 4));
        assertArrayEquals(new byte[]{3, 2, 1, 0, 7, 6, 5, 4}, blob.getBytes(1, arr.length));

        assertEquals(4, blob.setBytes(7, new byte[] {8, 9, 10, 11}, 0, 4));
        assertArrayEquals(new byte[]{3, 2, 1, 0, 7, 6, 8, 9, 10, 11}, blob.getBytes(1, (int)blob.length()));

        assertEquals(2, blob.setBytes(1, new byte[] {3, 2, 1, 0}, 2, 2));
        assertArrayEquals(new byte[]{1, 0, 1, 0, 7, 6, 8, 9, 10, 11}, blob.getBytes(1, (int)blob.length()));

        assertEquals(2, blob.setBytes(9, new byte[] {3, 2, 1, 0}, 1, 2));
        assertArrayEquals(new byte[]{1, 0, 1, 0, 7, 6, 8, 9, 2, 1}, blob.getBytes(1, (int)blob.length()));

        assertEquals(3, blob.setBytes(9, new byte[] {3, 2, 1, 0}, 0, 3));
        assertArrayEquals(new byte[]{1, 0, 1, 0, 7, 6, 8, 9, 3, 2, 1}, blob.getBytes(1, (int)blob.length()));

        JdbcBlob blob2 = new JdbcBlob(new byte[] {15, 16});
        assertEquals(8, blob2.setBytes(1, new byte[] {0, 1, 2, 3, 4, 5, 6, 7}, 0, 8));
        assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7}, blob2.getBytes(1, (int)blob2.length()));

        blob2.free();
        assertThrows(null, () -> blob2.setBytes(1, new byte[] {0, 1, 2}, 0, 2), SQLException.class, ERROR_BLOB_FREE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTruncate() throws Exception {
        JdbcBlob blob = new JdbcBlob();

        blob.setBytes(1, new byte[] {0});
        blob.setBytes(2, new byte[] {1, 2});
        blob.setBytes(4, new byte[] {3, 4, 5, 6});
        blob.setBytes(8, new byte[] {7, 8, 9, 10, 11, 12, 13, 14});
        blob.setBytes(16, new byte[] {15});

        assertThrows(null, () -> {
            blob.truncate(-1);

            return null;
        }, SQLException.class, null);

        assertThrows(null, () -> {
            blob.truncate(16 + 1);

            return null;
        }, SQLException.class, null);

        blob.truncate(12);
        assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, blob.getBytes(1, (int)blob.length()));

        blob.truncate(10);
        assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, blob.getBytes(1, (int)blob.length()));

        blob.truncate(0);
        assertEquals(0, (int)blob.length());

        blob.free();
        assertThrows(null, () -> {
            blob.truncate(0);

            return null;
        }, SQLException.class, ERROR_BLOB_FREE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFree() throws Exception {
        JdbcBlob blob = new JdbcBlob(new byte[] {0, 1, 2, 3, 4, 5, 6, 7});

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
        JdbcBlob blob = new JdbcBlob();

        assertThrows(null, () -> blob.setBinaryStream(0), SQLException.class, null);
        assertThrows(null, () -> blob.setBinaryStream(2L), SQLException.class, null);

        OutputStream os = blob.setBinaryStream(1L);
        os.write(0);
        os.write(new byte[] {1, 2, 3, 4, 5, 6, 7});
        os.close();
        assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7}, blob.getBytes(1, (int)blob.length()));

        os = blob.setBinaryStream(3L);
        os.write(new byte[] {20, 21, 22});
        os.write(23);
        os.close();
        assertArrayEquals(new byte[]{0, 1, 20, 21, 22, 23, 6, 7}, blob.getBytes(1, (int)blob.length()));

        os = blob.setBinaryStream(7L);
        os.write(new byte[] {30, 31, 32});
        os.write(33);
        os.close();
        assertArrayEquals(new byte[]{0, 1, 20, 21, 22, 23, 30, 31, 32, 33}, blob.getBytes(1, (int)blob.length()));

        blob.free();
        assertThrows(null, () -> blob.setBinaryStream(2L), SQLException.class, ERROR_BLOB_FREE);
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
