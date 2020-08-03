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
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.spi.encryption.EncryptionSpi;

/**
 * Factory to produce {@code EncryptedFileIO}.
 */
public class EncryptedFileIOFactory implements FileIOFactory {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Factory to produce underlying {@code FileIO} instances.
     */
    private FileIOFactory plainIOFactory;

    /**
     * Size of plain data page in bytes.
     */
    private int pageSize;

    /**
     * Size of file header in bytes.
     */
    private int headerSize;

    /**
     * Group id.
     */
    private int groupId;

    /**
     * Encryption manager.
     */
    private GridEncryptionManager encMgr;

    /**
     * Encryption spi.
     */
    private EncryptionSpi encSpi;

    /**
     * @param plainIOFactory Underlying file factory.
     * @param groupId Group id.
     * @param pageSize Size of plain data page in bytes.
     * @param encMgr Encryption manager.
     */
    EncryptedFileIOFactory(FileIOFactory plainIOFactory, int groupId, int pageSize, GridEncryptionManager encMgr,
        EncryptionSpi encSpi) {
        this.plainIOFactory = plainIOFactory;
        this.groupId = groupId;
        this.pageSize = pageSize;
        this.encMgr = encMgr;
        this.encSpi = encSpi;
    }

    /** {@inheritDoc} */
    @Override public FileIO create(File file, OpenOption... modes) throws IOException {
        FileIO io = plainIOFactory.create(file, modes);

        return new EncryptedFileIO(io, groupId, pageSize, headerSize, encMgr, encSpi);
    }

    /**
     * Sets size of file header in bytes.
     *
     * @param headerSize Size of file header in bytes.
     */
    void headerSize(int headerSize) {
        this.headerSize = headerSize;
    }
}
