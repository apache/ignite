/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import org.apache.ignite.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

import java.io.*;
import java.util.*;

/**
 * Tests fragmentizer work.
 */
public class GridGgfsFragmentizerSelfTest extends GridGgfsFragmentizerAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testReadFragmentizing() throws Exception {
        IgniteFs ggfs = grid(0).fileSystem("ggfs");

        IgniteFsPath path = new IgniteFsPath("/someFile");

        try (GridGgfsOutputStream out = ggfs.create(path, true)) {
            // Write 10 groups.
            for (int i = 0; i < 10 * GGFS_GROUP_SIZE; i++) {
                byte[] data = new byte[GGFS_BLOCK_SIZE];

                Arrays.fill(data, (byte)i);

                out.write(data);
            }
        }

        long start = System.currentTimeMillis();

        do {
            try (GridGgfsInputStream in = ggfs.open(path)) {
                for (int i = 0; i < 10 * GGFS_GROUP_SIZE; i++) {
                    for (int j = 0; j < GGFS_BLOCK_SIZE; j++)
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
        checkAppendFragmentizing(GGFS_BLOCK_SIZE / 4, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAppendFragmentizingAligned() throws Exception {
        checkAppendFragmentizing(GGFS_BLOCK_SIZE, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAppendFragmentizingDifferentNodes() throws Exception {
        checkAppendFragmentizing(GGFS_BLOCK_SIZE / 4, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAppendFragmentizingAlignedDifferentNodes() throws Exception {
        checkAppendFragmentizing(GGFS_BLOCK_SIZE, true);
    }

    /**
    * @throws Exception If failed.
    */
    private void checkAppendFragmentizing(int chunkSize, boolean rotate) throws Exception {
        IgniteFsPath path = new IgniteFsPath("/someFile");

        long written = 0;

        int i = 0;
        int ggfsIdx = 0;

        int fileSize = 30 * GGFS_GROUP_SIZE * GGFS_BLOCK_SIZE;

        while (written < fileSize) {
            IgniteFs ggfs = grid(ggfsIdx).fileSystem("ggfs");

            try (GridGgfsOutputStream out = ggfs.append(path, true)) {
                byte[] data = new byte[chunkSize];

                Arrays.fill(data, (byte)i);

                out.write(data);
            }

            System.out.println("Written [start=" + written + ", filler=" + i + ']');

            written += chunkSize;
            i++;

            if (rotate && i % 5 == 0) {
                ggfsIdx++;

                if (ggfsIdx >= NODE_CNT)
                    ggfsIdx = 0;
            }
        }

        IgniteFs ggfs = grid(0).fileSystem("ggfs");

        try (GridGgfsInputStream in = ggfs.open(path)) {
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
        checkFlushFragmentizing(GGFS_BLOCK_SIZE / 4);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFlushFragmentizingAligned() throws Exception {
        checkFlushFragmentizing(GGFS_BLOCK_SIZE);
    }

    /**
     * @param chunkSize Chunk size to test.
     * @throws Exception If failed.
     */
    private void checkFlushFragmentizing(int chunkSize) throws Exception {
        IgniteFsPath path = new IgniteFsPath("/someFile");

        long written = 0;
        int cnt = 0;

        int fileSize = 50 * GGFS_GROUP_SIZE * GGFS_BLOCK_SIZE;

        IgniteFs ggfs = grid(0).fileSystem("ggfs");

        byte[] chunk = new byte[chunkSize];

        while (written < fileSize) {
            try (GridGgfsOutputStream out = ggfs.append(path, true)) {
                for (int i = 0; i < 8; i++) {
                    Arrays.fill(chunk, (byte)cnt);

                    out.write(chunk);

                    out.flush();

                    written += chunkSize;

                    cnt++;
                }
            }
        }

        try (GridGgfsInputStream in = ggfs.open(path)) {
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
        GridGgfsImpl ggfs = (GridGgfsImpl)grid(0).fileSystem("ggfs");

        for (int i = 0; i < 30; i++) {
            IgniteFsPath path = new IgniteFsPath("/someFile" + i);

            try (GridGgfsOutputStream out = ggfs.create(path, true)) {
                for (int j = 0; j < 5 * GGFS_GROUP_SIZE; j++)
                    out.write(new byte[GGFS_BLOCK_SIZE]);
            }

            U.sleep(200);
        }

        ggfs.delete(new IgniteFsPath("/"), true);

        ggfs.awaitDeletesAsync().get();

        GridTestUtils.retryAssert(log, 50, 100, new CA() {
            @Override public void apply() {
                for (int i = 0; i < NODE_CNT; i++) {
                    GridEx g = grid(i);

                    GridCache<Object, Object> cache = g.cachex(DATA_CACHE_NAME);

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
