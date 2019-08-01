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

package org.apache.ignite.internal.managers.communication;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.util.IgniteUtils.assertParameter;

/**
 * Class represents a chunk data receiver which is pulling data from channel vi
 * {@link FileChannel#transferFrom(ReadableByteChannel, long, long)}.
 */
class FileReceiver extends AbstractReceiver {
    /** The default factory to provide IO oprations over underlying file. */
    @GridToStringExclude
    private final FileIOFactory fileIoFactory;

    /** Handler to notify when a file has been processed. */
    private final IgniteThrowableConsumer<File> hnd;

    /** The abstract java representation of the chunked file. */
    private File file;

    /** The corresponding file channel to work with. */
    @GridToStringExclude
    private FileIO fileIo;

    /**
     * @param nodeId The remote node id receive request for transmission from.
     * @param initMeta Initial file meta info.
     * @param stopChecker Node stop or prcoess interrupt checker.
     * @param factory Factory to produce IO interface on files.
     * @param hnd Transmission handler to process download result.
     * @param log Ignite logger.
     * @throws IgniteCheckedException If fails.
     */
    public FileReceiver(
        UUID nodeId,
        TransmissionMeta initMeta,
        int chunkSize,
        BooleanSupplier stopChecker,
        FileIOFactory factory,
        TransmissionHandler hnd,
        IgniteLogger log
    ) throws IgniteCheckedException {
        super(initMeta, stopChecker, log, chunkSize);

        assert initMeta.policy() == TransmissionPolicy.FILE : initMeta.policy();

        fileIoFactory = factory;
        this.hnd = hnd.fileHandler(nodeId, initMeta);

        assert this.hnd != null : "FileHandler must be provided by transmission handler";

        String fileAbsPath = hnd.filePath(nodeId, initMeta);

        if (fileAbsPath == null || fileAbsPath.trim().isEmpty())
            throw new IgniteCheckedException("File receiver absolute path cannot be empty or null. Receiver cannot be" +
                " initialized: " + this);

        file = new File(fileAbsPath);
    }

    /** {@inheritDoc} */
    @Override public void receive(
        ReadableByteChannel ch,
        TransmissionMeta meta
    ) throws IOException, IgniteCheckedException {
        super.receive(ch, meta);

        if (transferred == initMeta.count())
            hnd.accept(file);
    }

    /** {@inheritDoc} */
    @Override protected void init(TransmissionMeta meta) throws IgniteCheckedException {
        assert meta != null;
        assert fileIo == null;

        assertParameter(meta.name().equals(initMeta.name()), "Read operation stopped. " +
            "Attempt to receive a new file from channel, while the previous was not fully loaded " +
            "[meta=" + meta + ", prevFile=" + initMeta.name() + ']');

        try {
            fileIo = fileIoFactory.create(file);

            fileIo.position(initMeta.offset() + transferred);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Unable to open destination file. Receiver will will be stopped", e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void readChunk(ReadableByteChannel ch) throws IOException {
        long batchSize = Math.min(chunkSize, initMeta.count() - transferred);

        long readed = fileIo.transferFrom(ch, initMeta.offset() + transferred, batchSize);

        if (readed > 0)
            transferred += readed;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        U.closeQuiet(fileIo);

        fileIo = null;

        try {
            if (transferred != initMeta.count())
                Files.delete(file.toPath());
        }
        catch (IOException e) {
            U.error(log, "Error deleting not fully loaded file: " + file, e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FileReceiver.class, this, "super", super.toString());
    }
}
