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
import java.io.ObjectOutput;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.util.IgniteUtils.assertParameter;

/**
 * Class represents a data sender by chunks of predefined size. All of the chunks will be written to the
 * given socket channel. It is important that for each file you are going to send a new <em>FileSender</em>
 * instance will be created. The sender must keep its internal state of how much data already being
 * transferred to send its state to remote node when reconnection required.
 * <p>
 * The <em>FileSender</em> uses the zero-copy streaming algorithm, see <em>FileChannel#transferTo</em> for details.
 *
 * @see FileChannel#transferTo(long, long, WritableByteChannel)
 */
class FileSender extends AbstractTransmission {
    /** Default factory to provide IO oprations over given file. */
    @GridToStringExclude
    private final FileIOFactory fileIoFactory;

    /** File which will be send to remote by chunks. */
    private final File file;

    /** Corresponding file channel to work with given file. */
    @GridToStringExclude
    private FileIO fileIo;

    /**
     * @param file File which is going to be send by chunks.
     * @param pos File offset.
     * @param cnt Number of bytes to transfer.
     * @param params Additional file params.
     * @param stopChecker Node stop or prcoess interrupt checker.
     * @param log Ignite logger.
     * @param factory Factory to produce IO interface on given file.
     * @param chunkSize Size of chunks.
     */
    public FileSender(
        File file,
        long pos,
        long cnt,
        Map<String, Serializable> params,
        BooleanSupplier stopChecker,
        IgniteLogger log,
        FileIOFactory factory,
        int chunkSize
    ) {
        super(new TransmissionMeta(file.getName(), pos, cnt, params, null, null),
            stopChecker,
            log,
            chunkSize);

        assert file != null;

        this.file = file;
        fileIoFactory = factory;
    }

    /**
     * @param ch Output channel to write file to.
     * @param oo Channel to write meta info to.
     * @param connMeta Connection meta received.
     * @param plc Policy of how data will be handled on remote node.
     * @throws IOException If a transport exception occurred.
     * @throws IgniteCheckedException If fails.
     */
    public void send(WritableByteChannel ch,
        ObjectOutput oo,
        @Nullable TransmissionMeta connMeta,
        TransmissionPolicy plc
    ) throws IOException, IgniteCheckedException {
        try {
            // Can be not null if reconnection is going to be occurred.
            if (fileIo == null)
                fileIo = fileIoFactory.create(file);
        }
        catch (IOException e) {
            // Consider this IO exeption as a user one (not the network exception) and interrupt upload process.
            throw new IgniteCheckedException("Unable to initialize source file. File  sender upload will be stopped", e);
        }

        // If not the initial connection for the current session.
        if (connMeta != null)
            state(connMeta);

        // Write to remote about transission `is in active` mode.
        oo.writeBoolean(false);

        // Send meta about current file to remote.
        new TransmissionMeta(initMeta.name(),
            initMeta.offset() + transferred,
            initMeta.count() - transferred,
            initMeta.params(),
            plc,
            null)
            .writeExternal(oo);

        oo.flush();

        while (hasNextChunk()) {
            if (Thread.currentThread().isInterrupted() || stopped()) {
                throw new IgniteCheckedException("Thread has been interrupted or operation has been cancelled " +
                    "due to node is stopping. Channel processing has been stopped.");
            }

            writeChunk(ch);
        }

        assert transferred == initMeta.count() : "File is not fully transferred [expect=" + initMeta.count() +
            ", actual=" + transferred + ']';
    }

    /**
     * @param connMeta Conneciton meta info.
     * @throws IgniteCheckedException If fails.
     */
    private void state(TransmissionMeta connMeta) throws IgniteCheckedException {
        assert connMeta != null;
        assert fileIo != null;

        // Remote note doesn't have file info.
        if (connMeta.offset() < 0)
            return;

        long uploadedBytes = connMeta.offset() - initMeta.offset();

        assertParameter(initMeta.name().equals(connMeta.name()), "Attempt to transfer different file " +
            "while previous is not completed [initMeta=" + initMeta + ", meta=" + connMeta + ']');

        assertParameter(uploadedBytes >= 0, "Incorrect sync meta [offset=" + connMeta.offset() +
            ", initMeta=" + initMeta + ']');

        // No need to set new file position, if it is not changed.
        if (uploadedBytes == 0)
            return;

        transferred = uploadedBytes;

        U.log(log, "Update senders number of transferred bytes after reconnect: " + uploadedBytes);
    }

    /**
     * @param ch Channel to write data to.
     * @throws IOException If fails.
     */
    private void writeChunk(WritableByteChannel ch) throws IOException {
        long batchSize = Math.min(chunkSize, initMeta.count() - transferred);

        long sent = fileIo.transferTo(initMeta.offset() + transferred, batchSize, ch);

        if (sent > 0)
            transferred += sent;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        U.closeQuiet(fileIo);

        fileIo = null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FileSender.class, this, "super", super.toString());
    }
}
