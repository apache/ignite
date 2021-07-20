/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.storage.snapshot.local;

import java.io.File;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.BaseStorageTest;
import org.apache.ignite.raft.jraft.storage.FileService;
import org.apache.ignite.raft.jraft.storage.snapshot.Snapshot;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class LocalSnapshotReaderTest extends BaseStorageTest {
    private LocalSnapshotReader reader;
    @Mock
    private LocalSnapshotStorage snapshotStorage;
    private LocalSnapshotMetaTable table;
    private final int snapshotIndex = 99;

    @BeforeEach
    public void setup() throws Exception {
        RaftOptions opts = new RaftOptions();
        String snapPath = this.path + File.separator + Snapshot.JRAFT_SNAPSHOT_PREFIX + snapshotIndex;
        new File(snapPath).mkdirs();
        this.table = new LocalSnapshotMetaTable(opts);
        this.table.addFile("testFile", opts.getRaftMessagesFactory().localFileMeta().checksum("test").build());
        table.saveToFile(snapPath + File.separator + Snapshot.JRAFT_SNAPSHOT_META_FILE);
        this.reader = new LocalSnapshotReader(snapshotStorage, null, new Endpoint("localhost", 8081),
            opts, snapPath);
        assertTrue(this.reader.init(null));
    }

    @AfterEach
    public void teardown() throws Exception {
        this.reader.close();
        Mockito.verify(this.snapshotStorage, Mockito.only()).unref(this.snapshotIndex);
        assertFalse(FileService.getInstance().removeReader(this.reader.getReaderId()));
    }

    @Test
    public void testListFiles() {
        assertTrue(this.reader.listFiles().contains("testFile"));
    }

    @Test
    public void testGenerateUriForCopy() throws Exception {
        final String uri = this.reader.generateURIForCopy();
        assertNotNull(uri);
        assertTrue(uri.startsWith("remote://localhost:8081/"));
        final long readerId = Long.valueOf(uri.substring("remote://localhost:8081/".length()));
        assertTrue(FileService.getInstance().removeReader(readerId));
    }
}
