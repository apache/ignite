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
import java.io.Serializable;
import java.nio.file.OpenOption;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * {@link FileIO} factory definition.
 */
public interface FileIOFactory extends Serializable {
    /**
     * Creates I/O interface for file with default I/O mode.
     *
     * @param file File.
     * @return File I/O interface.
     * @throws IOException If I/O interface creation was failed.
     */
    default FileIO create(File file) throws IOException {
        return create(file, CREATE, READ, WRITE);
    }

    /**
     * Creates I/O interface for file with specified mode.
     *
     * @param file File
     * @param modes Open modes.
     * @return File I/O interface.
     * @throws IOException If I/O interface creation was failed.
     */
    FileIO create(File file, OpenOption... modes) throws IOException;
}
