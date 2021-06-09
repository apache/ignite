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

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import org.apache.ignite.raft.jraft.entity.LocalFileMetaOutter;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.BaseStorageTest;
import org.apache.ignite.raft.jraft.storage.snapshot.Snapshot;
import org.apache.ignite.raft.jraft.util.ByteBufferCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SnapshotFileReaderTest extends BaseStorageTest {
    private SnapshotFileReader reader;
    private LocalSnapshotMetaTable metaTable;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        this.reader = new SnapshotFileReader(path, null);
        metaTable = new LocalSnapshotMetaTable(new RaftOptions());
        this.reader.setMetaTable(metaTable);
    }

    @Test
    public void testReadMetaFile() throws Exception {
        final ByteBufferCollector bufRef = ByteBufferCollector.allocate(1024);
        final LocalFileMetaOutter.LocalFileMeta meta = addDataMeta();
        assertEquals(-1, this.reader.readFile(bufRef, Snapshot.JRAFT_SNAPSHOT_META_FILE, 0, Integer.MAX_VALUE));

        final ByteBuffer buf = bufRef.getBuffer();
        buf.flip();
        final LocalSnapshotMetaTable newTable = new LocalSnapshotMetaTable(new RaftOptions());
        newTable.loadFromIoBufferAsRemote(buf);
        Assert.assertEquals(meta, newTable.getFileMeta("data"));
    }

    private LocalFileMetaOutter.LocalFileMeta addDataMeta() {
        final LocalFileMetaOutter.LocalFileMeta meta = LocalFileMetaOutter.LocalFileMeta.newBuilder()
            .setChecksum("test").setSource(LocalFileMetaOutter.FileSource.FILE_SOURCE_LOCAL).build();
        this.metaTable.addFile("data", meta);
        return meta;
    }

    @Test
    public void testReadFile() throws Exception {
        final ByteBufferCollector bufRef = ByteBufferCollector.allocate();
        try {
            this.reader.readFile(bufRef, "unfound", 0, 1024);
            fail();
        }
        catch (final FileNotFoundException e) {
            // No-op.
        }

        final String data = writeData();
        addDataMeta();

        final int read = this.reader.readFile(bufRef, "data", 0, 1024);
        assertEquals(-1, read);
        final ByteBuffer buf = bufRef.getBuffer();
        buf.flip();
        assertEquals(data.length(), buf.remaining());
        final byte[] bs = new byte[data.length()];
        buf.get(bs);
        assertEquals(data, new String(bs));

    }
}
