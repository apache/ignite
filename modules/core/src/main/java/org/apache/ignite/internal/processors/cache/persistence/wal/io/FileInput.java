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

package org.apache.ignite.internal.processors.cache.persistence.wal.io;

import java.io.IOException;

import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInput;

/**
 * File input, backed by byte buffer file input.
 * This class allows to read data by chunks from file and then read primitives.
 */
public abstract class FileInput extends ByteBufferBackedDataInput {
    /**
     * File I/O.
     */
    public abstract FileIO io();

    /**
     * @param pos Position in bytes from file begin.
     */
    public abstract void seek(long pos) throws IOException;

    /** {@inheritDoc} */
    @Override public long size() throws IOException {
        return io().size();
    }
}
