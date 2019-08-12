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
import org.apache.ignite.IgniteException;
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
 * given socket channel. Please note, that for each file you are going to send a new {@link FileSender}
 * instance will be created. The sender must keep its internal state of how much data already being
 * transferred to send its state to remote node when reconnection required.
 * <p>
 * The <em>FileSender</em> uses the zero-copy streaming approach, see <em>FileChannel#transferTo</em> for details.
 *
 * @see FileChannel#transferTo(long, long, WritableByteChannel)
 */
class FileSender extends AbstractTransmission {
    /** Corresponding file channel to work with a given file. */
    @GridToStringExclude
    private FileIO fileIo;

    /**
     * @param file File which is going to be sent by chunks.
     * @param off File offset.
     * @param cnt Number of bytes to transfer.
     * @param params Additional file params.
     * @param plc Policy of handling data on remote.
     * @param stopChecker Node stop or process interrupt checker.
     * @param log Ignite logger.
     * @param factory Factory to produce IO interface on given file.
     * @param chunkSize Size of chunks.
     * @throws IOException If fails.
     */
    public FileSender(
        File file,
        long off,
        long cnt,
        Map<String, Serializable> params,
        TransmissionPolicy plc,
        BooleanSupplier stopChecker,
        IgniteLogger log,
        FileIOFactory factory,
        int chunkSize
    ) throws IOException {
        super(new TransmissionMeta(file.getName(), off, cnt, params, plc, null), stopChecker, log, chunkSize);

        assert file != null;

        fileIo = factory.create(file);
    }

    /**
     * @param ch Output channel to write file to.
     * @param oo Channel to write meta info to.
     * @param rcvMeta Connection meta received.
     * @throws IOException If a transport exception occurred.
     * @throws InterruptedException If thread interrupted.
     */
    public void send(WritableByteChannel ch,
        ObjectOutput oo,
        @Nullable TransmissionMeta rcvMeta
    ) throws IOException, InterruptedException {
        updateSenderState(rcvMeta);

        // Write flag to remote to keep currnet transmission opened.
        oo.writeBoolean(false);

        // Send meta about current file to remote.
        oo.writeObject(new TransmissionMeta(meta.name(),
            meta.offset() + transferred,
            meta.count() - transferred,
            meta.params(),
            meta.policy(),
            null));

        while (hasNextChunk()) {
            if (Thread.currentThread().isInterrupted())
                throw new InterruptedException("Sender thread has been interruped");

            if (stopped())
                throw new IgniteException("Sender has been cancelled due to the local node is stopping");

            writeChunk(ch);
        }

        assert transferred == meta.count() : "File is not fully transferred [expect=" + meta.count() +
            ", actual=" + transferred + ']';
    }

    /**
     * @param rcvMeta Conneciton meta info.
     */
    private void updateSenderState(TransmissionMeta rcvMeta) {
        // The remote node doesn't have a file meta info.
        if (rcvMeta == null || rcvMeta.offset() < 0) {
            transferred = 0;

            return;
        }

        long uploadedBytes = rcvMeta.offset() - meta.offset();

        assertParameter(meta.name().equals(rcvMeta.name()), "Attempt to transfer different file " +
            "while previous is not completed [meta=" + meta + ", meta=" + rcvMeta + ']');

        assertParameter(uploadedBytes >= 0, "Incorrect sync meta [offset=" + rcvMeta.offset() +
            ", meta=" + meta + ']');

        // No need to set new file position, if it is not changed.
        if (uploadedBytes == 0)
            return;

        transferred = uploadedBytes;

        U.log(log, "The number of transferred bytes after reconnect has been updated: " + uploadedBytes);
    }

    /**
     * @param ch Channel to write data to.
     * @throws IOException If fails.
     */
    private void writeChunk(WritableByteChannel ch) throws IOException {
        long batchSize = Math.min(chunkSize, meta.count() - transferred);

        long sent = fileIo.transferTo(meta.offset() + transferred, batchSize, ch);

        if (sent > 0)
            transferred += sent;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        U.closeQuiet(fileIo);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FileSender.class, this, "super", super.toString());
    }
}
