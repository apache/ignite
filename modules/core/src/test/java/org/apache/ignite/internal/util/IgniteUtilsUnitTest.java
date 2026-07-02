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

package org.apache.ignite.internal.util;

import java.io.File;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 * Unit tests for {@link IgniteUtils}.
 */
public class IgniteUtilsUnitTest {
    /***/
    private static final int PORT = 5555;

    /***/
    private final List<String> logMessages = new CopyOnWriteArrayList<>();

    /***/
    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    /***/
    @Test
    public void shouldNotProduceWarningsWhenClosingAnAlreadyClosedSocket() throws Exception {
        try (EchoServer server = new EchoServer(PORT)) {
            server.start();

            try (SocketChannel channel = connectTo(server)) {
                // closing first time
                channel.close();

                // now close second time and collect logs
                IgniteUtils.close(channel.socket(), logMessagesCollector());
            }
        }

        assertThat(logMessages, is(empty()));
    }

    /** Creating a directory that already exists must be a no-op, not an error. */
    @Test
    public void ensureDirectoryIsIdempotentForExistingDirectory() throws Exception {
        File dir = tmp.newFolder("dump_dir");

        IgniteUtils.ensureDirectory(dir, "dump directory", null);
        IgniteUtils.ensureDirectory(dir, "dump directory", null);
    }

    /** On failure the OS-level reason and cause must be reported, not a bare "can't be created". */
    @Test
    public void ensureDirectorySurfacesReasonAndCauseWhenPathComponentIsFile() throws Exception {
        File blocker = tmp.newFile("this_is_a_file");

        File target = new File(blocker, "db/cell_2_node_2");

        try {
            IgniteUtils.ensureDirectory(target, "dump directory", null);

            fail("Expected IgniteCheckedException");
        }
        catch (IgniteCheckedException e) {
            assertThat(e.getMessage(), containsString("Failed to create dump directory"));
            assertThat(e.getMessage(), containsString("reason="));
            assertThat(e.getCause(), instanceOf(IOException.class));
        }
    }

    /** A non-writable parent must yield an {@link AccessDeniedException}-backed message. */
    @Test
    public void ensureDirectoryReportsAccessDeniedWhenParentNotWritable() throws Exception {
        assumeTrue("POSIX permissions required", FileSystems.getDefault()
            .supportedFileAttributeViews().contains("posix"));

        File parent = tmp.newFolder("nvme5_snapshot");

        Files.setPosixFilePermissions(parent.toPath(), PosixFilePermissions.fromString("r-xr-xr-x"));

        try {
            // Skip when the current user can write regardless of the mode bits (e.g. running as root).
            assumeFalse("parent is writable (running as root?)", new File(parent, "probe").mkdirs());

            File target = new File(parent, "dump_x/db/cell_2_node_2");

            try {
                IgniteUtils.ensureDirectory(target, "dump directory", null);

                fail("Expected IgniteCheckedException");
            }
            catch (IgniteCheckedException e) {
                assertThat(e.getMessage(), containsString("AccessDeniedException"));
                assertThat(e.getCause(), instanceOf(AccessDeniedException.class));
            }
        }
        finally {
            Files.setPosixFilePermissions(parent.toPath(), PosixFilePermissions.fromString("rwxr-xr-x"));
        }
    }

    /***/
    private SocketChannel connectTo(EchoServer server) throws IOException {
        return SocketChannel.open(server.localSocketAddress());
    }

    /***/
    private ListeningTestLogger logMessagesCollector() {
        ListeningTestLogger log = new ListeningTestLogger();

        log.registerListener(logMessages::add);

        return log;
    }
}
