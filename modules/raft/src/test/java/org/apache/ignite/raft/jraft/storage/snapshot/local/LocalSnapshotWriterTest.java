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

import org.apache.ignite.raft.jraft.entity.LocalFileMetaOutter;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.BaseStorageTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(value = MockitoJUnitRunner.class)
public class LocalSnapshotWriterTest extends BaseStorageTest {
    private LocalSnapshotWriter writer;
    @Mock
    private LocalSnapshotStorage snapshotStorage;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        this.writer = new LocalSnapshotWriter(path, snapshotStorage, new RaftOptions());
        assertTrue(this.writer.init(null));
    }

    @Test
    public void testAddRemove() throws Exception {
        assertNull(this.writer.getFileMeta("data"));
        assertFalse(this.writer.listFiles().contains("data"));
        assertTrue(this.writer.addFile("data"));
        assertFalse(this.writer.addFile("data"));
        assertTrue(this.writer.listFiles().contains("data"));
        assertNotNull(this.writer.getFileMeta("data"));
        assertTrue(this.writer.removeFile("data"));
        assertFalse(this.writer.removeFile("data"));
        assertNull(this.writer.getFileMeta("data"));
        assertFalse(this.writer.listFiles().contains("data"));
    }

    @Test
    public void testSyncInit() throws Exception {
        LocalFileMetaOutter.LocalFileMeta meta = LocalFileMetaOutter.LocalFileMeta.newBuilder().setChecksum("test")
            .setSource(LocalFileMetaOutter.FileSource.FILE_SOURCE_LOCAL).build();
        assertTrue(this.writer.addFile("data1", meta));
        assertTrue(this.writer.addFile("data2"));

        assertEquals(meta, this.writer.getFileMeta("data1"));
        assertFalse(((LocalFileMetaOutter.LocalFileMeta) this.writer.getFileMeta("data2")).hasChecksum());
        assertFalse(((LocalFileMetaOutter.LocalFileMeta) this.writer.getFileMeta("data2")).hasUserMeta());

        this.writer.sync();
        //create a new writer
        LocalSnapshotWriter newWriter = new LocalSnapshotWriter(path, snapshotStorage, new RaftOptions());
        assertTrue(newWriter.init(null));
        assertNotSame(writer, newWriter);
        assertEquals(meta, newWriter.getFileMeta("data1"));
        assertFalse(((LocalFileMetaOutter.LocalFileMeta) newWriter.getFileMeta("data2")).hasChecksum());
        assertFalse(((LocalFileMetaOutter.LocalFileMeta) newWriter.getFileMeta("data2")).hasUserMeta());
    }

    @Test
    public void testClose() throws Exception {
        this.writer.close();
        Mockito.verify(this.snapshotStorage, Mockito.only()).close(this.writer, false);
    }
}
