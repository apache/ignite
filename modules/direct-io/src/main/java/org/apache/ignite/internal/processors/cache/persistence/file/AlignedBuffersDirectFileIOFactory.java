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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import net.smacke.jaydio.DirectIoLib;
import org.jsr166.ConcurrentHashMap8;

public class AlignedBuffersDirectFileIOFactory implements FileIOFactory {
    private final DirectIoLib directIoLib;
    private FileIOFactory backupFactory;
    private ConcurrentHashMap8<Long, Long> buffers;

    public AlignedBuffersDirectFileIOFactory(File storePath,
        FileIOFactory backupFactory) {
        this.backupFactory = backupFactory;
        directIoLib = DirectIoLib.getLibForPath(storePath.getAbsolutePath());
        //todo validate data storage settings and invalidate factory
    }

    @Override public FileIO create(File file) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override public FileIO create(File file, OpenOption... modes) throws IOException {
        if (directIoLib == null)
            return backupFactory.create(file, modes);


        return new AlignedBuffersDirectFileIO(  directIoLib.blockSize(), file, modes, buffers);
    }

    public void knownAlignedBuffers(ConcurrentHashMap8<Long, Long> buffers) {

        this.buffers = buffers;
    }
}
