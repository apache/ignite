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
import org.apache.ignite.raft.jraft.entity.RaftOutter;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.BaseStorageTest;
import org.apache.ignite.raft.jraft.storage.snapshot.Snapshot;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LocalSnapshotStorageTest extends BaseStorageTest {
    private LocalSnapshotStorage snapshotStorage;
    private LocalSnapshotMetaTable table;
    private int lastSnapshotIndex = 99;

    @Override
    @BeforeEach
    public void setup() throws Exception {
        super.setup();

        String snapshotPath = this.path + File.separator + Snapshot.JRAFT_SNAPSHOT_PREFIX + lastSnapshotIndex;
        new File(snapshotPath).mkdirs();
        this.table = new LocalSnapshotMetaTable(new RaftOptions());
        this.table.setMeta(RaftOutter.SnapshotMeta.newBuilder().setLastIncludedIndex(this.lastSnapshotIndex)
            .setLastIncludedTerm(1).build());
        this.table.saveToFile(snapshotPath + File.separator + Snapshot.JRAFT_SNAPSHOT_META_FILE);

        this.snapshotStorage = new LocalSnapshotStorage(path, new RaftOptions());
        assertTrue(this.snapshotStorage.init(null));
    }

    @Override
    @AfterEach
    public void teardown() throws Exception {
        super.teardown();
        this.snapshotStorage.shutdown();
    }

    @Test
    public void testGetLastSnapshotIndex() throws Exception {
        assertEquals(this.snapshotStorage.getLastSnapshotIndex(), lastSnapshotIndex);
        assertEquals(1, this.snapshotStorage.getRefs(this.lastSnapshotIndex).get());
    }

    @Test
    public void testCreateOpen() throws Exception {
        SnapshotWriter writer = this.snapshotStorage.create();
        assertNotNull(writer);
        RaftOutter.SnapshotMeta wroteMeta = RaftOutter.SnapshotMeta.newBuilder()
            .setLastIncludedIndex(this.lastSnapshotIndex + 1).setLastIncludedTerm(1).build();
        ((LocalSnapshotWriter) writer).saveMeta(wroteMeta);
        writer.addFile("data");
        assertEquals(1, this.snapshotStorage.getRefs(this.lastSnapshotIndex).get());
        writer.close();
        //release old
        assertEquals(0, this.snapshotStorage.getRefs(this.lastSnapshotIndex).get());
        //ref new
        assertEquals(1, this.snapshotStorage.getRefs(this.lastSnapshotIndex + 1).get());
        SnapshotReader reader = this.snapshotStorage.open();
        assertNotNull(reader);
        assertTrue(reader.listFiles().contains("data"));
        RaftOutter.SnapshotMeta readMeta = reader.load();
        assertEquals(wroteMeta, readMeta);
        assertEquals(2, this.snapshotStorage.getRefs(this.lastSnapshotIndex + 1).get());
        reader.close();
        assertEquals(1, this.snapshotStorage.getRefs(this.lastSnapshotIndex + 1).get());
    }

}
