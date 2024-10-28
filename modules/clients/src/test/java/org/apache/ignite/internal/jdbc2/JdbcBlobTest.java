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

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
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
        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7};

        JdbcBlob blob = new JdbcBlob(JdbcBinaryBuffer.createReadWrite(arr));
        doTestLength(blob, 8);

        blob = new JdbcBlob(JdbcBinaryBuffer.createReadWrite());
        doTestLength(blob, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLengthRO() throws Exception {
        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

        JdbcBlob blob = new JdbcBlob(JdbcBinaryBuffer.createReadOnly(arr, 4, 8));
        doTestLength(blob, 8);
    }

    public void doTestLength(Blob blob, int len) throws Exception {
        assertEquals(len, (int)blob.length());

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

        JdbcBlob blob = new JdbcBlob(JdbcBinaryBuffer.createReadWrite(arr));
        doTestGetBytes(blob);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetBytesRO() throws Exception {
        byte[] arr = new byte[] {-4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19};

        JdbcBlob blob = new JdbcBlob(JdbcBinaryBuffer.createReadOnly(arr, 4, 16));
        doTestGetBytes(blob);

        assertArrayEquals(new byte[] {-4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, arr);
    }

    /** */
    private void doTestGetBytes(Blob blob) throws Exception {
        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

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
        res = new byte[20];
        assertEquals(16, is.read(res, 0, 20));
        assertArrayEquals(arr, Arrays.copyOfRange(res, 0, 16));

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
        assertEquals(10, is.read(res, 1, 199));
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

        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        blob.setBytes(1, arr);

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

        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        blob.setBytes(1, arr);

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
        JdbcBlob blob = new JdbcBlob(JdbcBinaryBuffer.createReadWrite(
                new byte[] {0, 1, 2, 3, 4, 5, 6, 7}));

        doTestTruncate(blob, 8);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTruncateRO() throws Exception {
        byte[] arr = new byte[] {-4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        JdbcBlob blob = new JdbcBlob(JdbcBinaryBuffer.createReadOnly(arr, 4, 8));

        doTestTruncate(blob, 8);

        assertArrayEquals(new byte[] {-4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, arr);
    }

    /** */
    private void doTestTruncate(JdbcBlob blob, int len) throws Exception {
        try {
            blob.truncate(-1);

            fail();
        } catch (SQLException e) {
            // No-op.
        }

        try {
            blob.truncate(len + 1);

            fail();
        } catch (SQLException e) {
            // No-op.
        }

        blob.truncate(4);
        assertTrue(Arrays.equals(new byte[]{0, 1, 2, 3}, blob.getBytes(1, (int) blob.length())));

        blob.truncate(0);
        assertEquals(0, (int) blob.length());

        blob.free();

        try {
            blob.truncate(0);

            fail();
        } catch (SQLException e) {
            // No-op.
            System.out.println();
        }
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


// TODO move to BlobTest
//        is = blob.getBinaryStream();
//        assertEquals(1, is.read());
//        assertEquals(-1, is.read());
//
//        is = blob.getBinaryStream();
//        byte[] res = new byte[1];
//        assertEquals(1, is.read(res));
//        Assert.assertArrayEquals(new byte[] {1}, res);
//        assertEquals(-1, is.read(res));
//
//        is = blob.getBinaryStream(1, 1);
//        res = new byte[] {2, 2, 2};
//        assertEquals(1, is.read(res, 1, 2));
//        Assert.assertArrayEquals(new byte[] {2, 1, 2}, res);
//        assertEquals(-1, is.read(res, 2, 1));


//    /**
//     * @throws Exception If failed.
//     */
//    @Test
//    public void testBlobChangeAfterSelectOld() throws Exception {
//        byte[] res = new byte[1];
//
//        ResultSet rs = stmt.executeQuery(SQL);
//
//        assertTrue(rs.next());
//
//        Blob blob1 = rs.getBlob("blobVal");
//        Assert.assertArrayEquals(blob1.getBytes(1, (int)blob1.length()), new byte[] {1});
//
//        Blob blob2 = rs.getBlob("blobVal");
//        Blob blob3 = rs.getBlob("blobVal");
//        Blob blob4 = rs.getBlob("blobVal");
//        Blob blob5 = rs.getBlob("blobVal");
//
//        InputStream is1 = blob1.getBinaryStream();
//        assertEquals(1, is1.read());
//        assertEquals(-1, is1.read());
//        assertEquals(-1, is1.read(res));
//
//        OutputStream os1 = blob1.setBinaryStream(2);
//        os1.write(2);
//        assertEquals(2, is1.read());
//
//        InputStream is2 = blob2.getBinaryStream();
//
//        OutputStream os2 = blob2.setBinaryStream(2);
//        os2.write(new byte[] {3, 4});
//
//        assertEquals(1, is2.skip(1));
//        assertEquals(3, is2.read());
//        is2.mark(100);
//        assertEquals(4, is2.read());
//
//        assertEquals(2, blob2.setBytes(3, new byte[] {5, 6}));
//        is2.reset();
//        byte[] res2 = new byte[2];
//        assertEquals(2, is2.read(res2));
//        Assert.assertArrayEquals(new byte[] {5, 6}, res2);
//
//        assertEquals(0, is2.skip(2));
//        os2.write(new byte[] {7, 8});
//        is2.reset();
//        assertEquals(1, is2.skip(1));
//        assertEquals(7, is2.read());
//
//        InputStream is3 = blob3.getBinaryStream();
//        OutputStream os3 = blob3.setBinaryStream(2);
//        blob3.truncate(0);
//        assertEquals(0, blob3.length());
//        assertEquals(-1, is3.read());
//        assertThrows(null, () -> {
//            os3.write(55);
//
//            return null;
//        }, IOException.class, null);
//        assertThrows(null, () -> {
//            os3.write(new byte[] {9, 10});
//
//            return null;
//        }, IOException.class, null);
//
//        InputStream is4 = blob4.getBinaryStream();
//        blob4.setBytes(2, new byte[] {2, 3, 4, 5, 6, 7, 8, 9, 10});
//
//        assertEquals(2, is4.skip(2));
//        assertEquals(3, is4.read());
//        byte[] res4 = new byte[3];
//        is4.read(res4);
//        assertArrayEquals(new byte[] {4, 5, 6}, res4);
//        assertEquals(4, is4.skip(4));
//        assertEquals(-1, is4.read());
//        assertEquals(-1, is4.read(res4));
//
//        blob5.setBytes(2, new byte[] {2, 3, 4, 5});
//        InputStream is5 = blob5.getBinaryStream();
//        assertEquals(1, is5.read());
//        blob5.truncate(1);
//        byte[] res5 = new byte[1];
//        assertEquals(-1, is5.read(res5));
//    }

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
