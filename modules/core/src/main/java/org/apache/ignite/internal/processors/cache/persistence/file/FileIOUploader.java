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
import java.nio.channels.WritableByteChannel;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;

import static java.nio.file.StandardOpenOption.READ;

/** */
public class FileIOUploader {
    /** */
    private static final int CHUNK_SIZE = 1024 * 1024;

    /** */
    private final WritableByteChannel target;

    /** */
    private final FileIOFactory factory;

    /** */
    public FileIOUploader(WritableByteChannel target, FileIOFactory factory) {
        this.target = target;
        this.factory = factory;
    }

    /** */
    public void upload(File partFile) throws IgniteCheckedException {
        FileIO fileIO = null;

        try {
            fileIO = factory.create(partFile, READ);

            long written = 0;

            long size = fileIO.size();

            // Todo limit thransfer speed
            while (written < size)
                written += fileIO.transferTo(written, CHUNK_SIZE, target);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            U.closeQuiet(fileIO);
        }
    }
}
