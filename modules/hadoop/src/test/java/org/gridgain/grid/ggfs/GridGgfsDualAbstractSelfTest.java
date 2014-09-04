/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.ggfs.hadoop.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;

import static org.gridgain.grid.ggfs.GridGgfsMode.*;
import static org.gridgain.grid.ggfs.hadoop.GridGgfsHadoopParameters.*;

/**
 * Tests for GGFS working in mode when remote file system exists: DUAL_SYNC, DUAL_ASYNC.
 */
public abstract class GridGgfsDualAbstractSelfTest extends GridGgfsAbstractSelfTest {
    /** Secondary file system URI. */
    protected static final String SECONDARY_URI = "ggfs://ggfs-secondary:grid-secondary@127.0.0.1:11500/";

    /** Secondary file system configuration path. */
    protected static final String SECONDARY_CFG = "modules/core/src/test/config/hadoop/core-site-loopback-secondary.xml";

    /**
     * Constructor.
     *
     * @param mode GGFS mode.
     */
    protected GridGgfsDualAbstractSelfTest(GridGgfsMode mode) {
        super(mode);

        assert mode == DUAL_SYNC || mode == DUAL_ASYNC;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        chunk = new byte[128];

        for (int i = 0; i < chunk.length; i++)
            chunk[i] = (byte)i;

        Grid gridSecondary = startGridWithGgfs("grid-secondary", "ggfs-secondary", PRIMARY, null, SECONDARY_REST_CFG);

        GridGgfsFileSystem hadoopFs = new GridGgfsHadoopFileSystemWrapper(SECONDARY_URI, SECONDARY_CFG);

        Grid grid = startGridWithGgfs("grid", "ggfs", mode, hadoopFs, PRIMARY_REST_CFG);

        ggfsSecondary = (GridGgfsImpl)gridSecondary.ggfs("ggfs-secondary");
        ggfs = (GridGgfsImpl)grid.ggfs("ggfs");
    }

    /**
     * Ensure that no prefetch occurs in case not enough block are read sequentially.
     *
     * @throws Exception If failed.
     */
    public void testOpenNoPrefetch() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR), paths(FILE));

        // Write enough data to the secondary file system.
        final int blockSize = GGFS_BLOCK_SIZE;

        GridGgfsOutputStream out = ggfsSecondary.append(FILE, false);

        int totalWritten = 0;

        while (totalWritten < blockSize * 2 + chunk.length) {
            out.write(chunk);

            totalWritten += chunk.length;
        }

        out.close();

        awaitFileClose(ggfsSecondary, FILE);

        // Read the first block.
        int totalRead = 0;

        GridGgfsInputStream in = ggfs.open(FILE, blockSize);

        final byte[] readBuf = new byte[1024];

        while (totalRead + readBuf.length <= blockSize) {
            in.read(readBuf);

            totalRead += readBuf.length;
        }

        // Now perform seek.
        in.seek(blockSize * 2);

        // Read the third block.
        totalRead = 0;

        while (totalRead < totalWritten - blockSize * 2) {
            in.read(readBuf);

            totalRead += readBuf.length;
        }

        in.close();

        // Let's wait for a while because prefetch occurs asynchronously.
        U.sleep(200);

        // Remove the file from the secondary file system.
        ggfsSecondary.delete(FILE, false);

        // Try reading the second block. Should fail.
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                GridGgfsInputStream in0 = ggfs.open(FILE, 256);

                in0.seek(blockSize);

                try {
                    in0.read(readBuf);
                } finally {
                    U.closeQuiet(in0);
                }

                return null;
            }
        }, IOException.class,
            "Failed to read data due to secondary file system exception: /dir/subdir/file");
    }

    /**
     * Ensure that prefetch occurs in case several blocks are read sequentially.
     *
     * @throws Exception If failed.
     */
    public void testOpenPrefetch() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR), paths(FILE));

        // Write enough data to the secondary file system.
        final int blockSize = ggfs.info(FILE).blockSize();

        GridGgfsOutputStream out = ggfsSecondary.append(FILE, false);

        int totalWritten = 0;

        while (totalWritten < blockSize * 2 + chunk.length) {
            out.write(chunk);

            totalWritten += chunk.length;
        }

        out.close();

        awaitFileClose(ggfsSecondary, FILE);

        // Read the first two blocks.
        int totalRead = 0;

        GridGgfsInputStream in = ggfs.open(FILE, blockSize);

        final byte[] readBuf = new byte[1024];

        while (totalRead + readBuf.length <= blockSize * 2) {
            in.read(readBuf);

            totalRead += readBuf.length;
        }

        // Wait for a while for prefetch to finish.
        GridGgfsMetaManager meta = ggfs.context().meta();

        GridGgfsFileInfo info = meta.info(meta.fileId(FILE));

        GridGgfsBlockKey key = new GridGgfsBlockKey(info.id(), info.affinityKey(), info.evictExclude(), 2);

        GridCache<GridGgfsBlockKey, byte[]> dataCache = ggfs.context().kernalContext().cache().cache(
            ggfs.configuration().getDataCacheName());

        for (int i = 0; i < 10; i++) {
            if (dataCache.containsKey(key))
                break;
            else
                U.sleep(100);
        }

        in.close();

        // Remove the file from the secondary file system.
        ggfsSecondary.delete(FILE, false);

        // Read the third block.
        totalRead = 0;

        in = ggfs.open(FILE, blockSize);

        in.seek(blockSize * 2);

        while (totalRead + readBuf.length <= blockSize) {
            in.read(readBuf);

            totalRead += readBuf.length;
        }

        in.close();
    }

    /**
     * Check how prefetch override works.
     *
     * @throws Exception IF failed.
     */
    public void testOpenPrefetchOverride() throws Exception {
        create(ggfsSecondary, paths(DIR, SUBDIR), paths(FILE));

        // Write enough data to the secondary file system.
        final int blockSize = GGFS_BLOCK_SIZE;

        GridGgfsOutputStream out = ggfsSecondary.append(FILE, false);

        int totalWritten = 0;

        while (totalWritten < blockSize * 2 + chunk.length) {
            out.write(chunk);

            totalWritten += chunk.length;
        }

        out.close();

        awaitFileClose(ggfsSecondary, FILE);

        // Instantiate file system with overridden "seq reads before prefetch" property.
        Configuration cfg = new Configuration();

        cfg.addResource(U.resolveGridGainUrl(PRIMARY_CFG));

        int seqReads = SEQ_READS_BEFORE_PREFETCH + 1;

        cfg.setInt(String.format(PARAM_GGFS_SEQ_READS_BEFORE_PREFETCH, "ggfs:grid@"), seqReads);

        FileSystem fs = FileSystem.get(new URI(PRIMARY_URI), cfg);

        // Read the first two blocks.
        Path fsHome = new Path(PRIMARY_URI);
        Path dir = new Path(fsHome, DIR.name());
        Path subdir = new Path(dir, SUBDIR.name());
        Path file = new Path(subdir, FILE.name());

        FSDataInputStream fsIn = fs.open(file);

        final byte[] readBuf = new byte[blockSize * 2];

        fsIn.readFully(0, readBuf, 0, readBuf.length);

        // Wait for a while for prefetch to finish (if any).
        GridGgfsMetaManager meta = ggfs.context().meta();

        GridGgfsFileInfo info = meta.info(meta.fileId(FILE));

        GridGgfsBlockKey key = new GridGgfsBlockKey(info.id(), info.affinityKey(), info.evictExclude(), 2);

        GridCache<GridGgfsBlockKey, byte[]> dataCache = ggfs.context().kernalContext().cache().cache(
            ggfs.configuration().getDataCacheName());

        for (int i = 0; i < 10; i++) {
            if (dataCache.containsKey(key))
                break;
            else
                U.sleep(100);
        }

        fsIn.close();

        // Remove the file from the secondary file system.
        ggfsSecondary.delete(FILE, false);

        // Try reading the third block. Should fail.
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                GridGgfsInputStream in0 = ggfs.open(FILE);

                in0.seek(blockSize * 2);

                try {
                    in0.read(readBuf);
                }
                finally {
                    U.closeQuiet(in0);
                }

                return null;
            }
        }, IOException.class,
            "Failed to read data due to secondary file system exception: /dir/subdir/file");
    }
}
