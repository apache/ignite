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

package org.apache.ignite.internal.binary.compression;

import java.util.Arrays;
import org.apache.ignite.internal.binary.compression.Compressor;
import org.apache.ignite.internal.binary.compression.DeflaterCompressor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests of {@link DeflaterCompressor}.
 */
public class DeflaterCompressorSelfTest extends GridCommonAbstractTest {
    /** */
    private Compressor compressor;

    /** */
    String line = "абвгдеёжзийклмнопрстуфхцчшщъыьэюяАБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        compressor = new DeflaterCompressor();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStringCompress() throws Exception {
        byte[] compressed = compressor.compress(line.getBytes());
        byte[] decompressed = compressor.decompress(compressed);
        String decompressedLine = new String(decompressed);
        assertEquals(line, decompressedLine);
    }

    /**
     * @throws Exception If failed.
     */
    public void testByteArrayCompression() throws Exception {
        byte[] bytes = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14, 15};
        byte[] compressed = compressor.compress(bytes);
        byte[] decompressed = compressor.decompress(compressed);
        assertTrue(Arrays.equals(bytes, decompressed));
    }

    /**
     * @throws Exception If failed.
     */
    public void testByteArrayCompression2() throws Exception {
        byte[] bytes = new byte[] {33, 117, 4, -55, -16, 90, 1, 0, 0, 0, 0, 0, 0};
        byte[] compressed = compressor.compress(bytes);
        byte[] decompressed = compressor.decompress(compressed);
        assertTrue(Arrays.equals(bytes, decompressed));
    }
}