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

package org.apache.ignite.spi.swapspace.file;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for {@link FileSwapSpaceSpi}.
 */
public class GridFileSwapCompactionSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testCompact() throws Exception {
        File file = new File(UUID.randomUUID().toString());

        X.println("file: " + file.getPath());

        FileSwapSpaceSpi.SwapFile f = new FileSwapSpaceSpi.SwapFile(file, 8);

        Random rnd = new Random();

        ArrayList<FileSwapSpaceSpi.SwapValue> arr = new ArrayList<>();

        int size = 0;

        for (int a = 0; a < 100; a++) {
            FileSwapSpaceSpi.SwapValue[] vals = new FileSwapSpaceSpi.SwapValue[1 + rnd.nextInt(10)];

            int size0 = 0;

            for (int i = 0; i < vals.length; i++) {
                byte[] bytes = new byte[1 + rnd.nextInt(49)];

                rnd.nextBytes(bytes);

                size0 += bytes.length;

                vals[i] = new FileSwapSpaceSpi.SwapValue(bytes);

                arr.add(vals[i]);
            }

            f.write(new FileSwapSpaceSpi.SwapValues(vals, size0), 1);

            size += size0;

            assertEquals(f.length(), size);
            assertEquals(file.length(), size);
        }

        int i = 0;

        for (FileSwapSpaceSpi.SwapValue val : arr)
            assertEquals(val.idx(), ++i);

        i = 0;

        for (int cnt = arr.size() / 2; i < cnt; i++) {

            FileSwapSpaceSpi.SwapValue v = arr.remove(rnd.nextInt(arr.size()));

            assertTrue(f.tryRemove(v.idx(), v));
        }

        int hash0 = 0;

        for (FileSwapSpaceSpi.SwapValue val : arr)
            hash0 += Arrays.hashCode(val.readValue(f.readCh));

        ArrayList<T2<ByteBuffer, ArrayDeque<FileSwapSpaceSpi.SwapValue>>> bufs = new ArrayList();

        for (;;) {
            ArrayDeque<FileSwapSpaceSpi.SwapValue> que = new ArrayDeque<>();

            ByteBuffer buf = f.compact(que, 1024);

            if (buf == null)
                break;

            bufs.add(new T2(buf, que));
        }

        f.delete();

        int hash1 = 0;

        for (FileSwapSpaceSpi.SwapValue val : arr)
            hash1 += Arrays.hashCode(val.value(null));

        assertEquals(hash0, hash1);

        File file0 = new File(UUID.randomUUID().toString());

        FileSwapSpaceSpi.SwapFile f0 = new FileSwapSpaceSpi.SwapFile(file0, 8);

        for (T2<ByteBuffer, ArrayDeque<FileSwapSpaceSpi.SwapValue>> t : bufs)
            f0.write(t.get2(), t.get1(), 1);

        int hash2 = 0;

        for (FileSwapSpaceSpi.SwapValue val : arr)
            hash2 += Arrays.hashCode(val.readValue(f0.readCh));

        assertEquals(hash2, hash1);
    }
}