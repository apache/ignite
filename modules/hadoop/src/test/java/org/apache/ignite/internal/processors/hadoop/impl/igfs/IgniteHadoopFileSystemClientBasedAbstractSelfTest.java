/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.hadoop.impl.igfs;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * IGFS Hadoop file system Ignite client -based self test.
 */
public abstract class IgniteHadoopFileSystemClientBasedAbstractSelfTest extends IgniteHadoopFileSystemAbstractSelfTest {
    /** Alive node index. */
    private static final int ALIVE_NODE_IDX = GRID_COUNT - 1;

    /**
     * Constructor.
     *
     * @param mode IGFS mode.
     */
    IgniteHadoopFileSystemClientBasedAbstractSelfTest(IgfsMode mode) {
        super(mode, true, true);
    }

    /** {@inheritDoc} */
    @Override protected IgfsIpcEndpointConfiguration primaryIpcEndpointConfiguration(final String gridName) {
        return null;
    }

    /** {@inheritDoc} */
    @Override protected FileSystemConfiguration igfsConfiguration(String gridName) throws IgniteCheckedException {
        FileSystemConfiguration cfg = super.igfsConfiguration(gridName);

        cfg.setIpcEndpointEnabled(false);

        return cfg;
    }

    /**
     * @return Path to Ignite client node configuration.
     */
    protected abstract String getClientConfig();

    /** {@inheritDoc} */
    @Override protected Configuration configuration(String authority, boolean skipEmbed, boolean skipLocShmem) {
        Configuration cfg = new Configuration();

        cfg.set("fs.defaultFS", "igfs://" + authority + "/");
        cfg.set("fs.igfs.impl", IgniteHadoopFileSystem.class.getName());
        cfg.set("fs.AbstractFileSystem.igfs.impl",
            org.apache.ignite.hadoop.fs.v2.IgniteHadoopFileSystem.class.getName());

        cfg.setBoolean("fs.igfs.impl.disable.cache", true);
        cfg.setBoolean(String.format(HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_EMBED, authority), true);
        cfg.setBoolean(String.format(HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_LOCAL_SHMEM, authority), true);
        cfg.setBoolean(String.format(HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_LOCAL_TCP, authority), true);
        cfg.setBoolean(String.format(HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_REMOTE_TCP, authority), true);
        cfg.setStrings(String.format(HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_IGNITE_CFG_PATH, authority),
            getClientConfig());

        return cfg;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testClientReconnect() throws Exception {
        Path filePath = new Path(PRIMARY_URI, "file1");

        final FSDataOutputStream s = fs.create(filePath); // Open the stream before stopping IGFS.

        try {
            restartServerNodesExceptOne();

            // Check that client is again operational.
            assertTrue(fs.mkdirs(new Path(PRIMARY_URI, "dir1/dir2")));

            s.write("test".getBytes());

            s.flush(); // Flush data to the broken output stream.

            assertTrue(fs.exists(filePath));
        }
        finally {
            U.closeQuiet(s); // Safety.
        }
    }

    /**
     * Verifies that client reconnects after connection to the server has been lost (multithreaded mode).
     *
     * @throws Exception If error occurs.
     */
    @Test
    @Override public void testClientReconnectMultithreaded() throws Exception {
        final ConcurrentLinkedQueue<FileSystem> q = new ConcurrentLinkedQueue<>();

        Configuration cfg = new Configuration();

        for (Map.Entry<String, String> entry : primaryFsCfg)
            cfg.set(entry.getKey(), entry.getValue());

        cfg.setBoolean("fs.igfs.impl.disable.cache", true);

        final int nClients = 1;

        // Initialize clients.
        for (int i = 0; i < nClients; i++)
            q.add(FileSystem.get(primaryFsUri, cfg));

        restartServerNodesExceptOne();

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                FileSystem fs = q.poll();

                try {
                    // Check that client is again operational.
                    assertTrue(fs.mkdirs(new Path("/" + Thread.currentThread().getName())));

                    return true;
                }
                finally {
                    U.closeQuiet(fs);
                }
            }
        }, nClients, "test-client");
    }

    /**
     *
     * @throws Exception If failed.
     */
    private void restartServerNodesExceptOne() throws Exception {
        stopAllNodesExcept(ALIVE_NODE_IDX);

        Thread.sleep(500);

        startAllNodesExcept(ALIVE_NODE_IDX); // Start server again.

        Thread.sleep(500);

        stopGrid(ALIVE_NODE_IDX);

        Thread.sleep(500);

        startGrid(ALIVE_NODE_IDX);

        Thread.sleep(500);
    }

    /**
     * @param nodeIdx Node index to not stop
     */
    private void stopAllNodesExcept(int nodeIdx) {
        for (int i = 0; i < GRID_COUNT; ++i)
            if (i != nodeIdx)
                stopGrid(i);
    }

    /**
     * @param nodeIdx Node index to not stop
     * @throws Exception If failed.
     */
    private void startAllNodesExcept(int nodeIdx) throws Exception {
        for (int i = 0; i < GRID_COUNT; ++i)
            if (i != nodeIdx)
                startGrid(i);
    }
}
