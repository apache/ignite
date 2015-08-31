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

package org.apache.ignite.internal.client.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.util.GridClientByteUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import static org.apache.ignite.internal.util.GridClientByteUtils.bytesToInt;
import static org.apache.ignite.internal.util.GridClientByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.GridClientByteUtils.bytesToShort;
import static org.apache.ignite.internal.util.GridClientByteUtils.intToBytes;
import static org.apache.ignite.internal.util.GridClientByteUtils.longToBytes;
import static org.apache.ignite.internal.util.GridClientByteUtils.shortToBytes;

/**
 * Test case for client's byte convertion utility.
 */
public class ClientByteUtilsTest extends GridCommonAbstractTest {
    /**
     * Test UUID conversions from string to binary and back.
     *
     * @throws Exception On any exception.
     */
    public void testUuidConvertions() throws Exception {
        Map<String, byte[]> map = new LinkedHashMap<>();

        map.put("2ec84557-f7c4-4a2e-aea8-251eb13acff3", new byte[] {
            46, -56, 69, 87, -9, -60, 74, 46, -82, -88, 37, 30, -79, 58, -49, -13
        });
        map.put("4e17b7b5-79e7-4db5-ac45-a644ead95b9e", new byte[] {
            78, 23, -73, -75, 121, -25, 77, -75, -84, 69, -90, 68, -22, -39, 91, -98
        });
        map.put("412daadb-e9e6-443b-8b87-8d7895fc2e53", new byte[] {
            65, 45, -86, -37, -23, -26, 68, 59, -117, -121, -115, 120, -107, -4, 46, 83
        });
        map.put("e71aabf4-4aad-4280-b4e9-3c310be0cb88", new byte[] {
            -25, 26, -85, -12, 74, -83, 66, -128, -76, -23, 60, 49, 11, -32, -53, -120
        });
        map.put("d4454cda-a81f-490f-9424-9bdfcc9cf610", new byte[] {
            -44, 69, 76, -38, -88, 31, 73, 15, -108, 36, -101, -33, -52, -100, -10, 16
        });
        map.put("3a584450-5e85-4b69-9f9d-043d89fef23b", new byte[] {
            58, 88, 68, 80, 94, -123, 75, 105, -97, -99, 4, 61, -119, -2, -14, 59
        });
        map.put("6c8baaec-f173-4a60-b566-240a87d7f81d", new byte[] {
            108, -117, -86, -20, -15, 115, 74, 96, -75, 102, 36, 10, -121, -41, -8, 29
        });
        map.put("d99c7102-79f7-4fb4-a665-d331cf285c20", new byte[] {
            -39, -100, 113, 2, 121, -9, 79, -76, -90, 101, -45, 49, -49, 40, 92, 32
        });
        map.put("007d56c7-5c8b-4279-a700-7f3f95946dde", new byte[] {
            0, 125, 86, -57, 92, -117, 66, 121, -89, 0, 127, 63, -107, -108, 109, -34
        });
        map.put("15627963-d8f9-4423-bedc-f6f89f7d3433", new byte[] {
            21, 98, 121, 99, -40, -7, 68, 35, -66, -36, -10, -8, -97, 125, 52, 51
        });

        for (Map.Entry<String, byte[]> e : map.entrySet()) {
            UUID uuid = UUID.fromString(e.getKey());
            UUID uuidFromBytes = GridClientByteUtils.bytesToUuid(e.getValue(), 0);

            assertEquals(uuid, uuidFromBytes);
            assertEquals(e.getKey(), uuid.toString());
            assertEquals(e.getKey(), uuidFromBytes.toString());

            byte[] bytes = new byte[16];

            GridClientByteUtils.uuidToBytes(uuid, bytes, 0);

            assertTrue(e.getKey(), Arrays.equals(e.getValue(), bytes));
        }
    }

    public void testShortToBytes() throws Exception {
        Map<String, Short> map = new HashMap<>();

        map.put("00-00", (short)0);
        map.put("00-0F", (short)0x0F);
        map.put("FF-F1", (short)-0x0F);
        map.put("27-10", (short)10000);
        map.put("D8-F0", (short)-10000);
        map.put("80-00", Short.MIN_VALUE);
        map.put("7F-FF", Short.MAX_VALUE);

        for (Map.Entry<String, Short> entry : map.entrySet()) {
            byte[] b = asByteArray(entry.getKey());

            Assert.assertArrayEquals(b, shortToBytes(entry.getValue()));
            Assert.assertEquals((short)entry.getValue(), bytesToShort(b, 0));

            byte[] tmp = new byte[2];

            shortToBytes(entry.getValue(), tmp, 0);
            Assert.assertArrayEquals(b, tmp);
        }
    }

    public void testIntToBytes() throws Exception {
        Map<String, Integer> map = new HashMap<>();

        map.put("00-00-00-00", 0);
        map.put("00-FF-FF-FF", 0xFFFFFF);
        map.put("FF-00-00-01", -0xFFFFFF);
        map.put("3B-9A-CA-00", 1000000000);
        map.put("C4-65-36-00", -1000000000);
        map.put("80-00-00-00", Integer.MIN_VALUE);
        map.put("7F-FF-FF-FF", Integer.MAX_VALUE);

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            byte[] b = asByteArray(entry.getKey());

            Assert.assertArrayEquals(b, intToBytes(entry.getValue()));
            Assert.assertEquals((int)entry.getValue(), bytesToInt(b, 0));

            byte[] tmp = new byte[4];

            intToBytes(entry.getValue(), tmp, 0);
            Assert.assertArrayEquals(b, tmp);
        }
    }

    public void testLongToBytes() throws Exception {
        Map<String, Long> map = new LinkedHashMap<>();

        map.put("00-00-00-00-00-00-00-00", 0L);
        map.put("00-00-00-00-00-FF-FF-FF", 0xFFFFFFL);
        map.put("FF-FF-FF-FF-FF-00-00-01", -0xFFFFFFL);
        map.put("00-00-00-00-3B-9A-CA-00", 1000000000L);
        map.put("FF-FF-FF-FF-C4-65-36-00", -1000000000L);
        map.put("00-00-AA-AA-AA-AA-AA-AA", 0xAAAAAAAAAAAAL);
        map.put("FF-FF-55-55-55-55-55-56", -0xAAAAAAAAAAAAL);
        map.put("0D-E0-B6-B3-A7-64-00-00", 1000000000000000000L);
        map.put("F2-1F-49-4C-58-9C-00-00", -1000000000000000000L);
        map.put("80-00-00-00-00-00-00-00", Long.MIN_VALUE);
        map.put("7F-FF-FF-FF-FF-FF-FF-FF", Long.MAX_VALUE);

        for (Map.Entry<String, Long> entry : map.entrySet()) {
            byte[] b = asByteArray(entry.getKey());

            Assert.assertArrayEquals(b, longToBytes(entry.getValue()));
            Assert.assertEquals((long)entry.getValue(), bytesToLong(b, 0));

            byte[] tmp = new byte[8];

            longToBytes(entry.getValue(), tmp, 0);
            Assert.assertArrayEquals(b, tmp);
        }
    }

    private byte[] asByteArray(String text) {
        String[] split = text.split("-");
        byte[] b = new byte[split.length];

        for (int i = 0; i < split.length; i++)
            b[i] = (byte)Integer.parseInt(split[i], 16);

        return b;
    }
}