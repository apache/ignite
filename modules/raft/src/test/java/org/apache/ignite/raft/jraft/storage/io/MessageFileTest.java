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
package org.apache.ignite.raft.jraft.storage.io;

import java.io.File;
import org.apache.ignite.raft.jraft.entity.LocalFileMetaOutter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MessageFileTest {
    @Test
    public void testSaveLoad() throws Exception {
        File tempFile = File.createTempFile("test", "msgfile");
        String path = tempFile.getAbsolutePath();
        tempFile.delete();
        MessageFile file = new MessageFile(path);
        assertNull(file.load());
        LocalFileMetaOutter.LocalFileMeta msg = LocalFileMetaOutter.LocalFileMeta.newBuilder().setChecksum("test")
            .setSource(LocalFileMetaOutter.FileSource.FILE_SOURCE_REFERENCE).build();
        assertTrue(file.save(msg, true));

        MessageFile newFile = new MessageFile(path);
        LocalFileMetaOutter.LocalFileMeta loadedMsg = newFile.load();
        assertNotNull(loadedMsg);
        assertEquals("test", loadedMsg.getChecksum());
        assertEquals(LocalFileMetaOutter.FileSource.FILE_SOURCE_REFERENCE, loadedMsg.getSource());

        new File(path).delete();
        assertNull(newFile.load());
    }
}
