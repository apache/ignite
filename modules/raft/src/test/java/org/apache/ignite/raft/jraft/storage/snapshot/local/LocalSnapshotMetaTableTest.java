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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.raft.jraft.entity.LocalFileMetaOutter;
import org.apache.ignite.raft.jraft.entity.RaftOutter;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(WorkDirectoryExtension.class)
public class LocalSnapshotMetaTableTest {
    private LocalSnapshotMetaTable table;

    @BeforeEach
    public void setup() {
        this.table = new LocalSnapshotMetaTable(new RaftOptions());
    }

    @Test
    public void testAddRemove() {
        LocalFileMetaOutter.LocalFileMeta meta = LocalFileMetaOutter.LocalFileMeta.newBuilder().setChecksum("test")
            .setSource(LocalFileMetaOutter.FileSource.FILE_SOURCE_LOCAL).build();
        assertEquals(0, table.listFiles().size());
        assertTrue(this.table.addFile("data", meta));
        assertFalse(this.table.addFile("data", meta));

        assertEquals(1, table.listFiles().size());
        assertTrue(table.listFiles().contains("data"));

        assertTrue(this.table.removeFile("data"));
        assertFalse(this.table.removeFile("data"));
        assertEquals(0, table.listFiles().size());
    }

    @Test
    public void testSaveLoadFile(@WorkDirectory Path workDir) throws IOException {
        LocalFileMetaOutter.LocalFileMeta meta1 = LocalFileMetaOutter.LocalFileMeta.newBuilder().setChecksum("data1")
            .setSource(LocalFileMetaOutter.FileSource.FILE_SOURCE_LOCAL).build();
        assertTrue(this.table.addFile("data1", meta1));
        LocalFileMetaOutter.LocalFileMeta meta2 = LocalFileMetaOutter.LocalFileMeta.newBuilder().setChecksum("data2")
            .setSource(LocalFileMetaOutter.FileSource.FILE_SOURCE_LOCAL).build();
        assertTrue(this.table.addFile("data2", meta2));

        RaftOutter.SnapshotMeta meta = RaftOutter.SnapshotMeta.newBuilder().setLastIncludedIndex(1).setLastIncludedTerm(1).build();
        this.table.setMeta(meta);

        assertTrue(table.listFiles().contains("data1"));
        assertTrue(table.listFiles().contains("data2"));
        assertTrue(table.hasMeta());

        String filePath = workDir.resolve("table").toString();
        table.saveToFile(filePath);

        LocalSnapshotMetaTable newTable = new LocalSnapshotMetaTable(new RaftOptions());
        assertNull(newTable.getFileMeta("data1"));
        assertNull(newTable.getFileMeta("data2"));
        assertTrue(newTable.loadFromFile(filePath));
        assertEquals(meta1, newTable.getFileMeta("data1"));
        assertEquals(meta2, newTable.getFileMeta("data2"));
        assertEquals(meta, newTable.getMeta());
    }

    @Test
    public void testSaveLoadIoBuffer() throws Exception {
        LocalFileMetaOutter.LocalFileMeta meta1 = LocalFileMetaOutter.LocalFileMeta.newBuilder().setChecksum("data1")
            .setSource(LocalFileMetaOutter.FileSource.FILE_SOURCE_LOCAL).build();
        assertTrue(this.table.addFile("data1", meta1));
        LocalFileMetaOutter.LocalFileMeta meta2 = LocalFileMetaOutter.LocalFileMeta.newBuilder().setChecksum("data2")
            .setSource(LocalFileMetaOutter.FileSource.FILE_SOURCE_LOCAL).build();
        assertTrue(this.table.addFile("data2", meta2));

        ByteBuffer buf = this.table.saveToByteBufferAsRemote();
        assertNotNull(buf);
        assertTrue(buf.hasRemaining());

        LocalSnapshotMetaTable newTable = new LocalSnapshotMetaTable(new RaftOptions());
        assertNull(newTable.getFileMeta("data1"));
        assertNull(newTable.getFileMeta("data2"));
        assertTrue(newTable.loadFromIoBufferAsRemote(buf));
        assertEquals(meta1, newTable.getFileMeta("data1"));
        assertEquals(meta2, newTable.getFileMeta("data2"));
    }
}
