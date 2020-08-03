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
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Class represents the data receiver which is pulling data from channel using
 * {@link FileChannel#transferFrom(ReadableByteChannel, long, long)} until the
 * whole file will be completely received.
 */
class FileReceiver extends TransmissionReceiver {
    /** Handler to notify when a file has been received. */
    private final Consumer<File> hnd;

    /** File to receive data into. */
    private File file;

    /** The corresponding file channel to work with. */
    @GridToStringExclude
    private FileIO fileIo;

    /**
     * @param meta Initial file meta info.
     * @param stopChecker Node stop or prcoess interrupt checker.
     * @param factory Factory to produce IO interface on files.
     * @param hnd Transmission handler provider to process download result.
     * @param path File path to destination receiver source.
     * @param log Ignite logger.
     */
    public FileReceiver(
        TransmissionMeta meta,
        int chunkSize,
        BooleanSupplier stopChecker,
        FileIOFactory factory,
        Consumer<File> hnd,
        String path,
        IgniteLogger log
    ) {
        super(meta, stopChecker, log, chunkSize);

        A.notNull(hnd, "FileHandler must be provided by transmission handler");
        A.notNull(path, "File absolute path cannot be null");
        A.ensure(!path.trim().isEmpty(), "File absolute path cannot be empty ");

        this.hnd = hnd;

        file = new File(path);

        try {
            fileIo = factory.create(file);

            fileIo.position(meta.offset());
        }
        catch (IOException e) {
            throw new IgniteException("Unable to open destination file. Receiver will will be stopped", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void receive(ReadableByteChannel ch) throws IOException, InterruptedException {
        super.receive(ch);

        if (transferred == meta.count())
            hnd.accept(file);
    }

    /** {@inheritDoc} */
    @Override protected void readChunk(ReadableByteChannel ch) throws IOException {
        assert fileIo != null;

        long batchSize = Math.min(chunkSize, meta.count() - transferred);

        long read = fileIo.transferFrom(ch, meta.offset() + transferred, batchSize);

        if (read == 0)
            throw new IOException("Channel is reached the end of stream. Probably, channel is closed on the remote node");

        if (read > 0)
            transferred += read;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        U.closeQuiet(fileIo);

        try {
            if (transferred != meta.count())
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
