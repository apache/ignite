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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import com.sun.nio.file.ExtendedOpenOption;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;

/**
 * File I/O factory which provides {@link BufferedFileIO} implementation of FileIO.
 */
public class DirectBufferedFileIOFactory implements FileIOFactory {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final FileIOFactory factory;

    /** */
    private final int bufSz;

    /** */
    public DirectBufferedFileIOFactory(FileIOFactory factory, int bufSz) {
        this.factory = factory;
        this.bufSz = bufSz;
    }

    /** {@inheritDoc} */
    @Override public DirectBufferedFileIO create(File file, OpenOption... modes) throws IOException {
        ArrayList list = new ArrayList(Arrays.asList(modes));

        list.add(ExtendedOpenOption.DIRECT);

        FileIO io = factory.create(file, modes);

        int blockSize = (int)Files.getFileStore(file.toPath()).getBlockSize();

        return new DirectBufferedFileIO(io, blockSize);
    }
}
