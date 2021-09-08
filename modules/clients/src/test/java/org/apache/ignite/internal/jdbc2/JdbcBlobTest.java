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
import java.sql.SQLException;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** */
public class JdbcBlobTest {
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
    public void testPositionBytePattern() throws Exception {
        byte[] arr = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

        JdbcBlob blob = new JdbcBlob(arr);

        assertEquals(-1, blob.position(new byte[] {1, 2, 3}, 0));
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

        assertEquals(-1, blob.position(new JdbcBlob(new byte[] {1, 2, 3}), 0));
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
        catch (ArrayIndexOutOfBoundsException e) {
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
        catch (ArrayIndexOutOfBoundsException e) {
            // No-op.
        }

        try {
            blob.setBytes(1, new byte[4], -1, 2);

            fail();
        }
        catch (ArrayIndexOutOfBoundsException e) {
            // No-op.
        }

        try {
            blob.setBytes(1, new byte[4], 0, 5);

            fail();
        }
        catch (ArrayIndexOutOfBoundsException e) {
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
