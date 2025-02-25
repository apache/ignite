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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.encryption.GroupKey;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.configuration.DataStorageConfiguration.MAX_PAGE_SIZE;

/**
 * File for recovery on failure during checkpoint.
 * Not thread safe.
 */
public class CheckpointRecoveryFile implements AutoCloseable {
    /** Record header size. */
    private static final int HEADER_SIZE =
        8 /* pageId */ +
        4 /* grpId */ +
        4 /* pageSize */ +
        4 /* CRC */ +
        1 /* encrypted flag */ +
        1 /* encryption key id */;

    /** Record header buffer. */
    private final ByteBuffer hdrBuf = ByteBuffer.allocateDirect(HEADER_SIZE).order(ByteOrder.nativeOrder());

    /** Buffer for encrypted data. */
    private final ByteBuffer encBuf;

    /** Buffer for file writes caching. */
    private final ByteBuffer writeBuf;

    /** Context. */
    private final GridKernalContext ctx;

    /** Encryption SPI */
    private final EncryptionSpi encSpi;

    /** File IO to read/write recovery data. */
    private final FileIO fileIo;

    /** Checkpoint timestamp. */
    private final long cpTs;

    /** Checkpoint ID. */
    private final UUID cpId;

    /** Checkpointer index. */
    private final int cpIdx;

    /** */
    private final File file;

    /**
     * Ctor.
     */
    CheckpointRecoveryFile(GridKernalContext ctx, long cpTs, UUID cpId, int cpIdx, File file, FileIO fileIo) {
        this.ctx = ctx;
        this.cpTs = cpTs;
        this.cpId = cpId;
        this.cpIdx = cpIdx;
        this.file = file;
        this.fileIo = fileIo;

        writeBuf = ByteBuffer.allocateDirect(ctx.cache().context().database().pageSize()).order(ByteOrder.nativeOrder());
        encSpi = ctx.encryption().enabled() ? ctx.config().getEncryptionSpi() : null;
        encBuf = encSpi != null ? ByteBuffer.allocateDirect(encSpi.encryptedSize(MAX_PAGE_SIZE)) : null;
    }

    /** Checkpoint timestamp. */
    public long checkpointTs() {
        return cpTs;
    }

    /** Checkpoint ID. */
    public UUID checkpointId() {
        return cpId;
    }

    /** Checkpointer index. */
    public int checkpointerIndex() {
        return cpIdx;
    }

    /** Recovery file. */
    public File file() {
        return file;
    }

    /** */
    public void fsync() throws IOException {
        if (writeBuf.position() > 0) {
            writeBuf.flip();
            fileIo.writeFully(writeBuf);
            writeBuf.clear();
        }

        fileIo.force();
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        if (writeBuf.position() > 0) {
            writeBuf.flip();
            fileIo.writeFully(writeBuf);
        }

        fileIo.close();
    }

    /** */
    private void write(ByteBuffer buf) throws IOException {
        do {
            if (buf.remaining() > writeBuf.remaining()) {
                int oldLimit = buf.limit();

                buf.limit(buf.position() + writeBuf.remaining());

                writeBuf.put(buf);

                buf.limit(oldLimit);
            }
            else
                writeBuf.put(buf);

            if (writeBuf.remaining() == 0) {
                writeBuf.rewind();

                fileIo.writeFully(writeBuf);

                writeBuf.clear();
            }
        }
        while (buf.remaining() > 0);
    }

    /** */
    public void writePage(FullPageId fullPageId, ByteBuffer buf) throws IOException {
        // Encrypt if required.
        byte encFlag = 0;
        byte encKeyId = 0;

        if (encSpi != null) {
            GroupKey grpKey = ctx.encryption().getActiveKey(fullPageId.groupId());

            if (grpKey != null) {
                encFlag = 1;
                encKeyId = grpKey.id();
                encBuf.clear();
                encBuf.limit(encSpi.encryptedSize(buf.remaining()));
                encSpi.encrypt(buf, grpKey.key(), encBuf);
                encBuf.rewind();
                buf = encBuf;
            }
        }

        hdrBuf.clear();
        hdrBuf.putLong(fullPageId.pageId());
        hdrBuf.putInt(fullPageId.groupId());
        hdrBuf.putInt(buf.remaining());
        // We have dedicated CRC field in the page structure, but we intentionally store CRC to record header, since
        // page buffer can be encrypted.
        hdrBuf.putInt(FastCrc.calcCrc(buf, buf.remaining()));
        hdrBuf.put(encFlag);
        hdrBuf.put(encKeyId);

        hdrBuf.rewind();
        buf.rewind();

        write(hdrBuf);
        write(buf);
    }

    /** */
    private @Nullable FullPageId readPage(Predicate<FullPageId> pageIdPredicate, ByteBuffer buf) throws IOException {
        // Read header.
        hdrBuf.clear();
        long pos = fileIo.position(); // For error messages.

        int read = fileIo.readFully(hdrBuf);

        if (read <= 0)
            return null;

        if (read < hdrBuf.capacity()) {
            throw new IOException("Recovery file buffer underflow [file=" + file.getName() +
                ", pos=" + pos + ", expSize=" + hdrBuf.capacity() + ", read=" + read + ']');
        }

        hdrBuf.rewind();
        FullPageId fullPageId = new FullPageId(hdrBuf.getLong(), hdrBuf.getInt());
        int pageSize = hdrBuf.getInt();
        int storedCrc = hdrBuf.getInt();
        byte encFlag = hdrBuf.get();
        byte encKeyId = hdrBuf.get();

        if (pageSize <= 0 || pageSize + fileIo.position() > fileIo.size()) {
            throw new IOException("Unexpected page size [file=" + file.getName() +
                ", pos=" + pos + ", pageSize=" + pageSize + ']');
        }

        if (!pageIdPredicate.test(fullPageId)) {
            fileIo.position(fileIo.position() + pageSize);
            buf.clear();
            buf.limit(0);
            return fullPageId;
        }

        ByteBuffer decBuf = buf; // Buffer for decrypted data.

        if (encFlag != 0)
            buf = encBuf;

        if (pageSize > buf.capacity()) {
            throw new IOException("Unexpected page size [file=" + file.getName() +
                ", pos=" + pos + ", pageSize=" + pageSize + ']');
        }

        // Read page data.
        buf.clear();
        buf.limit(pageSize);
        read = fileIo.readFully(buf);

        if (read < pageSize) {
            throw new IOException("Recovery file buffer underflow [file=" + file.getName() +
                ", pos=" + pos + ", expSize=" + pageSize + ", read=" + read + ']');
        }

        buf.rewind();

        int calcCrc = FastCrc.calcCrc(buf, buf.remaining());

        if (storedCrc != calcCrc) {
            throw new IOException("CRC validation failed [file=" + file.getName() + ", pos=" + pos +
                ", storedCrc=" + U.hexInt(storedCrc) + ", calcCrc=" + U.hexInt(calcCrc) + "]");
        }

        buf.rewind();

        if (encFlag != 0) {
            if (encSpi == null) {
                throw new IOException("Found encrypted record, but encryption is disabled [file=" +
                    file.getName() + ", pos=" + pos + ']');
            }

            GroupKey grpKey = ctx.encryption().groupKey(fullPageId.groupId(), encKeyId);

            if (grpKey == null) {
                throw new IOException("Not found encryption key id [file=" +
                    file.getName() + ", pos=" + pos + ", grpId=" + fullPageId.groupId() + ", keyId=" + encKeyId + ']');
            }

            decBuf.clear();
            encSpi.decrypt(buf, grpKey.key(), decBuf);
            decBuf.limit(decBuf.position());
            decBuf.rewind();
        }

        return fullPageId;
    }

    /**
     * Preforms action on all pages stored in recovery file.
     */
    public void forAllPages(
        Predicate<FullPageId> pageIdPredicate,
        BiConsumer<FullPageId, ByteBuffer> action
    ) throws IOException {
        fileIo.position(0);

        ByteBuffer buf = ByteBuffer.allocateDirect(MAX_PAGE_SIZE).order(ByteOrder.nativeOrder());

        FullPageId fullPageId = readPage(pageIdPredicate, buf);

        while (fullPageId != null) {
            // Correct fullPageId (but with empty buffer) still will be returned, even when fullPageId
            // doesn't satisfy pageIdPredicate.
            if (pageIdPredicate.test(fullPageId))
                action.accept(fullPageId, buf);

            fullPageId = readPage(pageIdPredicate, buf);
        }
    }
}
