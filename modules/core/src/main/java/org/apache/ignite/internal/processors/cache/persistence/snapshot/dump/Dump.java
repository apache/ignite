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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.zip.ZipInputStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadata;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.jetbrains.annotations.Nullable;

import static java.nio.file.StandardOpenOption.READ;
import static org.apache.ignite.internal.processors.cache.GridLocalConfigManager.readCacheData;
import static org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree.dumpPartFileName;
import static org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree.snapshotMetaFile;
import static org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree.snapshotMetaFileName;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext.closeAllComponents;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext.startAllComponents;

/**
 * This class provides the ability to work with saved cache dump.
 */
public class Dump implements AutoCloseable {
    /** Snapshot meta. */
    private final List<SnapshotMetadata> metadata;

    /** Dump directory. */
    private final File dumpDir;

    /** Dump directories. */
    private final List<SnapshotFileTree> sfts;

    /** Specific consistent id. */
    private final @Nullable String consistentId;

    /** Kernal context for each node in dump. */
    private final GridKernalContext cctx;

    /** If {@code true} then return data in form of {@link BinaryObject}. */
    private final boolean keepBinary;

    /** If {@code true} then don't deserialize {@link KeyCacheObject} and {@link CacheObject}. */
    private final boolean raw;

    /** Encryption SPI. */
    private final @Nullable EncryptionSpi encSpi;

    /**
     * Map shared across all instances of {@link DumpEntrySerializer}.
     * We use per thread buffer because number of threads is fewer then number of partitions.
     * Regular count of partitions is {@link RendezvousAffinityFunction#DFLT_PARTITION_COUNT}
     * and thread is {@link IgniteConfiguration#DFLT_PUBLIC_THREAD_CNT} whic is significantly less.
     */
    private final ConcurrentMap<Long, ByteBuffer> thLocBufs = new ConcurrentHashMap<>();

    /** If {@code true} then compress partition files. */
    private final boolean comprParts;

    /**
     * @param dumpDir Dump directory.
     * @param keepBinary If {@code true} then keep read entries in binary form.
     * @param raw If {@code true} then keep read entries in form of {@link KeyCacheObject} and {@link CacheObject}.
     * @param log Logger.
     */
    public Dump(File dumpDir, boolean keepBinary, boolean raw, IgniteLogger log) {
        this(dumpDir, null, keepBinary, raw, null, log);
    }

    /**
     * @param dumpDir Dump directory.
     * @param consistentId If specified, read dump data only for specific node.
     * @param keepBinary If {@code true} then keep read entries in binary form.
     * @param raw If {@code true} then keep read entries in form of {@link KeyCacheObject} and {@link CacheObject}.
     * @param encSpi Encryption SPI instance.
     * @param log Logger.
     */
    public Dump(
        File dumpDir,
        @Nullable String consistentId,
        boolean keepBinary,
        boolean raw,
        @Nullable EncryptionSpi encSpi,
        IgniteLogger log
    ) {
        A.ensure(dumpDir != null, "dump directory is null");
        A.ensure(dumpDir.exists(), "dump directory not exists");

        this.dumpDir = dumpDir;
        this.consistentId = consistentId == null ? null : U.maskForFileName(consistentId);
        this.metadata = metadata(dumpDir, this.consistentId);
        this.keepBinary = keepBinary;
        this.cctx = standaloneKernalContext(dumpDir, F.first(metadata).folderName(), log);
        this.sfts = metadata.stream()
            .map(m -> new SnapshotFileTree(cctx, m.snapshotName(), dumpDir.getParent(), m.folderName(), m.consistentId()))
            .collect(Collectors.toList());
        this.raw = raw;
        this.encSpi = encSpi;
        this.comprParts = metadata.get(0).compressPartitions();

        for (SnapshotMetadata meta : metadata) {
            if (meta.encryptionKey() != null && encSpi == null)
                throw new IllegalArgumentException("Encryption SPI required to read encrypted dump");
        }
    }

    /**
     * @param log Logger.
     * @return Standalone kernal context.
     */
    private static GridKernalContext standaloneKernalContext(File root, String folderName, IgniteLogger log) {
        NodeFileTree ft = new NodeFileTree(root, folderName);

        A.ensure(ft.binaryMeta().exists(), "binary metadata directory not exists");
        A.ensure(ft.marshaller().exists(), "marshaller directory not exists");

        try {
            GridKernalContext kctx = new StandaloneGridKernalContext(log, ft.binaryMeta(), ft.marshaller());

            startAllComponents(kctx);

            return kctx;
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** @return Binary types iterator. */
    public Iterator<BinaryType> types() {
        return cctx.cacheObjects().metadata().iterator();
    }

    /** @return List of snapshot metadata saved in {@link #dumpDir}. */
    public List<SnapshotMetadata> metadata() {
        return Collections.unmodifiableList(metadata);
    }

    /** @return List of snapshot metadata saved in {@link #dumpDir}. */
    private static List<SnapshotMetadata> metadata(File dumpDir, @Nullable String consistentId) {
        JdkMarshaller marsh = new JdkMarshaller();

        ClassLoader clsLdr = U.resolveClassLoader(new IgniteConfiguration());

        // First filter only specific file to exclude overlapping with other nodes making dump on the local host.
        File[] files = dumpDir.listFiles(
            f -> snapshotMetaFile(f) && (consistentId == null || f.getName().equals(snapshotMetaFileName(consistentId)))
        );

        if (files == null)
            return Collections.emptyList();

        return Arrays.stream(files)
            .map(meta -> {
                try (InputStream in = new BufferedInputStream(Files.newInputStream(meta.toPath()))) {
                    return marsh.<SnapshotMetadata>unmarshal(in, clsLdr);
                }
                catch (IOException | IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            })
            .filter(SnapshotMetadata::dump)
            .collect(Collectors.toList());
    }

    /**
     * @param node Node directory name.
     * @param grp Group id.
     * @return List of cache configs saved in dump for group.
     */
    public List<StoredCacheData> configs(String node, int grp) {
        JdkMarshaller marsh = cctx.marshallerContext().jdkMarshaller();

        return NodeFileTree.cacheConfigFiles(sft(node).cacheDirectory(grp)).stream().map(f -> {
            try {
                return readCacheData(f, marsh, cctx.config());
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }).collect(Collectors.toList());
    }

    /**
     * @param node Node directory name.
     * @param grp Group id.
     * @return Dump iterator.
     */
    public List<Integer> partitions(String node, int grp) {
        List<File> parts = sft(node).cachePartitionFiles(sft(node).cacheDirectory(grp), true, comprParts);

        if (parts == null)
            return Collections.emptyList();

        return parts.stream()
            .map(NodeFileTree::partId)
            .collect(Collectors.toList());
    }

    /**
     * @param node Node directory name.
     * @param grp Group id.
     * @return Dump iterator.
     */
    public DumpedPartitionIterator iterator(String node, int grp, int part) {
        FileIOFactory ioFactory = comprParts
            ? (file, modes) -> new ReadOnlyUnzipFileIO(file)
            : (file, modes) -> new ReadOnlyBufferedFileIO(file);

        FileIO dumpFile;

        try {
            dumpFile = ioFactory.create(new File(sft(node).cacheDirectory(grp), dumpPartFileName(part, comprParts)));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        SnapshotMetadata meta = metadata.stream().filter(m -> Objects.equals(m.folderName(), node)).findFirst().orElseThrow();

        boolean encrypted = meta.encryptionKey() != null;

        if (encrypted && !Arrays.equals(meta.masterKeyDigest(), encSpi.masterKeyDigest())) {
            throw new IllegalStateException("Dump '" +
                meta.snapshotName() + "' has different master key digest. To restore this " +
                "dump, provide the same master key.");
        }

        DumpEntrySerializer serializer = new DumpEntrySerializer(
            thLocBufs,
            encrypted ? new ConcurrentHashMap<>() : null,
            encrypted ? encSpi.decryptKey(meta.encryptionKey()) : null,
            encSpi
        );

        serializer.kernalContext(cctx);
        serializer.keepBinary(keepBinary);
        serializer.raw(raw);

        return new DumpedPartitionIterator() {
            DumpEntry next;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                advance();

                return next != null;
            }

            /** {@inheritDoc} */
            @Override public DumpEntry next() {
                advance();

                if (next == null)
                    throw new NoSuchElementException();

                DumpEntry next0 = next;

                next = null;

                return next0;
            }

            /** */
            private void advance() {
                if (next != null)
                    return;

                try {
                    next = serializer.read(dumpFile, grp, part);
                }
                catch (IOException | IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }

            /** {@inheritDoc} */
            @Override public void close() {
                U.closeQuiet(dumpFile);
            }
        };
    }

    /** @return Root dump directory. */
    public File dumpDirectory() {
        return dumpDir;
    }

    /** @return Dump directories. */
    public List<SnapshotFileTree> fileTrees() {
        return sfts;
    }

    /** @return Snapshot file tree for specific folder name. */
    private SnapshotFileTree sft(String folderName) {
        return sfts.stream().filter(sft -> sft.folderName().equals(folderName)).findFirst().orElseThrow();
    }

    /** @return Kernal context. */
    public GridKernalContext context() {
        return cctx;
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        closeAllComponents(cctx);

        if (encSpi != null)
            encSpi.spiStop();
    }

    /**
     * Closeable dump iterator.
     */
    public interface DumpedPartitionIterator extends Iterator<DumpEntry>, AutoCloseable {
        // No-op.
    }

    /** */
    private static class ReadOnlyBufferedFileIO extends FileIODecorator {
        /** */
        private static final int DEFAULT_BLOCK_SIZE = 4096;

        /** */
        private final ByteBuffer buf;

        /** */
        private long pos;

        /** */
        ReadOnlyBufferedFileIO(File file) throws IOException {
            super(new RandomAccessFileIO(file, READ));

            int blockSize = getFileSystemBlockSize();

            if (blockSize <= 0)
                blockSize = DEFAULT_BLOCK_SIZE;

            buf = ByteBuffer.allocateDirect(blockSize);

            buf.position(buf.limit());
        }

        /** {@inheritDoc} */
        @Override public int readFully(ByteBuffer dst) throws IOException {
            int totalRead = 0;

            while (dst.hasRemaining()) {
                if (!buf.hasRemaining()) {
                    // Buf limit will be at its capacity unless partial fill has happened at the end of file.
                    if (buf.limit() < buf.capacity())
                        break;

                    buf.clear();

                    pos += delegate.readFully(buf, pos);

                    buf.flip();
                }

                int len = Math.min(buf.remaining(), dst.remaining());

                int limit = buf.limit();

                buf.limit(buf.position() + len);

                dst.put(buf);

                buf.limit(limit);

                totalRead += len;
            }

            return totalRead;
        }

    }

    /** */
    private static class ReadOnlyUnzipFileIO extends ReadOnlyBufferedFileIO {
        /** */
        private final ZipInputStream zis;

        /** */
        ReadOnlyUnzipFileIO(File file) throws IOException {
            super(file);

            zis = new ZipInputStream(new InputStream() {
                /** {@inheritDoc} */
                @Override public int read(byte[] arr, int off, int len) throws IOException {
                    return ReadOnlyUnzipFileIO.super.readFully(ByteBuffer.wrap(arr, off, len));
                }

                /** {@inheritDoc} */
                @Override public int read() throws IOException {
                    throw new IOException();
                }
            });

            zis.getNextEntry();
        }

        /** {@inheritDoc} */
        @Override public int readFully(ByteBuffer dst) throws IOException {
            int totalRead = 0;

            while (dst.hasRemaining()) {
                int bytesRead = zis.read(dst.array(), dst.arrayOffset() + dst.position(), dst.remaining());

                if (bytesRead == -1)
                    break;

                dst.position(dst.position() + bytesRead);

                totalRead += bytesRead;
            }

            return totalRead;
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            zis.close();
        }
    }
}
