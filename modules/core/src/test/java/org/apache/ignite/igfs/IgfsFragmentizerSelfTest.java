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

package org.apache.ignite.igfs;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.igfs.IgfsImpl;
import org.apache.ignite.internal.util.typedef.CA;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Tests fragmentizer work.
 */
public class IgfsFragmentizerSelfTest extends IgfsFragmentizerAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testReadFragmentizing() throws Exception {
        IgniteFileSystem igfs = grid(0).fileSystem("igfs");

        IgfsPath path = new IgfsPath("/someFile");

        try (IgfsOutputStream out = igfs.create(path, true)) {
            // Write 10 groups.
            for (int i = 0; i < 10 * IGFS_GROUP_SIZE; i++) {
                byte[] data = new byte[IGFS_BLOCK_SIZE];

                Arrays.fill(data, (byte)i);

                out.write(data);
            }
        }

        long start = System.currentTimeMillis();

        do {
            try (IgfsInputStream in = igfs.open(path)) {
                for (int i = 0; i < 10 * IGFS_GROUP_SIZE; i++) {
                    for (int j = 0; j < IGFS_BLOCK_SIZE; j++)
                        assertEquals(i & 0xFF, in.read());
                }

                assertEquals(-1, in.read());
            }
        }
        while (System.currentTimeMillis() - start < 7000);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAppendFragmentizing() throws Exception {
        checkAppendFragmentizing(IGFS_BLOCK_SIZE / 4, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAppendFragmentizingAligned() throws Exception {
        checkAppendFragmentizing(IGFS_BLOCK_SIZE, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAppendFragmentizingDifferentNodes() throws Exception {
        checkAppendFragmentizing(IGFS_BLOCK_SIZE / 4, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAppendFragmentizingAlignedDifferentNodes() throws Exception {
        checkAppendFragmentizing(IGFS_BLOCK_SIZE, true);
    }

    /**
    * @throws Exception If failed.
    */
    private void checkAppendFragmentizing(int chunkSize, boolean rotate) throws Exception {
        IgfsPath path = new IgfsPath("/someFile");

        long written = 0;

        int i = 0;
        int igfsIdx = 0;

        int fileSize = 30 * IGFS_GROUP_SIZE * IGFS_BLOCK_SIZE;

        while (written < fileSize) {
            IgniteFileSystem igfs = grid(igfsIdx).fileSystem("igfs");

            try (IgfsOutputStream out = igfs.append(path, true)) {
                byte[] data = new byte[chunkSize];

                Arrays.fill(data, (byte)i);

                out.write(data);
            }

            System.out.println("Written [start=" + written + ", filler=" + i + ']');

            written += chunkSize;
            i++;

            if (rotate && i % 5 == 0) {
                igfsIdx++;

                if (igfsIdx >= NODE_CNT)
                    igfsIdx = 0;
            }
        }

        IgniteFileSystem igfs = grid(0).fileSystem("igfs");

        try (IgfsInputStream in = igfs.open(path)) {
            i = 0;

            int read = 0;

            byte[] chunk = new byte[chunkSize];

            while (read < fileSize) {
                readFully(in, chunk);

                for (byte b : chunk)
                    assertEquals("For read offset [start=" + read + ", filler=" + (i & 0xFF) + ']',
                        i & 0xFF, b & 0xFF);

                i++;

                read += chunkSize;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testFlushFragmentizing() throws Exception {
        checkFlushFragmentizing(IGFS_BLOCK_SIZE / 4);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFlushFragmentizingAligned() throws Exception {
        checkFlushFragmentizing(IGFS_BLOCK_SIZE);
    }

    /**
     * @param chunkSize Chunk size to test.
     * @throws Exception If failed.
     */
    private void checkFlushFragmentizing(int chunkSize) throws Exception {
        IgfsPath path = new IgfsPath("/someFile");

        long written = 0;
        int cnt = 0;

        int fileSize = 50 * IGFS_GROUP_SIZE * IGFS_BLOCK_SIZE;

        IgniteFileSystem igfs = grid(0).fileSystem("igfs");

        byte[] chunk = new byte[chunkSize];

        while (written < fileSize) {
            try (IgfsOutputStream out = igfs.append(path, true)) {
                for (int i = 0; i < 8; i++) {
                    Arrays.fill(chunk, (byte)cnt);

                    out.write(chunk);

                    out.flush();

                    written += chunkSize;

                    cnt++;
                }
            }
        }

        try (IgfsInputStream in = igfs.open(path)) {
            cnt = 0;

            int read = 0;

            while (read < fileSize) {
                readFully(in, chunk);

                for (byte b : chunk)
                    assertEquals("For read offset [start=" + read + ", filler=" + (cnt & 0xFF) + ']',
                        cnt & 0xFF, b & 0xFF);

                cnt++;

                read += chunkSize;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testDeleteFragmentizing() throws Exception {
        IgfsImpl igfs = (IgfsImpl)grid(0).fileSystem("igfs");

        for (int i = 0; i < 30; i++) {
            IgfsPath path = new IgfsPath("/someFile" + i);

            try (IgfsOutputStream out = igfs.create(path, true)) {
                for (int j = 0; j < 5 * IGFS_GROUP_SIZE; j++)
                    out.write(new byte[IGFS_BLOCK_SIZE]);
            }

            U.sleep(200);
        }

        igfs.delete(new IgfsPath("/"), true);

        igfs.awaitDeletesAsync().get();

        GridTestUtils.retryAssert(log, 50, 100, new CA() {
            @Override public void apply() {
                for (int i = 0; i < NODE_CNT; i++) {
                    IgniteEx g = grid(i);

                    GridCacheAdapter<Object, Object> cache = ((IgniteKernal)g).internalCache(DATA_CACHE_NAME);

                    assertTrue("Data cache is not empty [keys=" + cache.keySet() +
                        ", node=" + g.localNode().id() + ']', cache.isEmpty());
                }
            }
        });
    }

    /**
     * @param in Input stream to read from.
     * @param data Byte array to read to.
     * @throws IOException If read failed.
     */
    private static void readFully(InputStream in, byte[] data) throws IOException {
        int read = 0;

        while(read < data.length)
            read += in.read(data, read, data.length - read);
    }
}