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
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
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
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotMetadata;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_BINARY_METADATA_PATH;
import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_MARSHALLER_PATH;
import static org.apache.ignite.internal.processors.cache.GridLocalConfigManager.readCacheData;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_METAFILE_EXT;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.CreateDumpFutureTask.DUMP_FILE_EXT;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext.closeAllComponents;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext.startAllComponents;

/**
 * This class provides ability to work with saved cache dump.
 */
public class Dump implements AutoCloseable {
    /** Snapshot meta. */
    private final List<SnapshotMetadata> metadata;

    /** Dump directory. */
    private final File dumpDir;

    /** Specific consistent id. */
    private final @Nullable String consistentId;

    /** Kernal context for each node in dump. */
    private final GridKernalContext cctx;

    /** If {@code true} then return data in form of {@link BinaryObject}. */
    private final boolean keepBinary;

    /** If {@code true} then don't deserialize {@link KeyCacheObject} and {@link CacheObject}. */
    private final boolean raw;

    /**
     * Map shared across all instances of {@link DumpEntrySerializer}.
     * We use per thread buffer because number of threads is fewer then number of partitions.
     * Regular count of partitions is {@link RendezvousAffinityFunction#DFLT_PARTITION_COUNT}
     * and thread is {@link IgniteConfiguration#DFLT_PUBLIC_THREAD_CNT} whic is significantly less.
     */
    private final ConcurrentMap<Long, ByteBuffer> thLocBufs = new ConcurrentHashMap<>();

    /**
     * @param dumpDir Dump directory.
     * @param keepBinary If {@code true} then keep read entries in binary form.
     * @param raw If {@code true} then keep read entries in form of {@link KeyCacheObject} and {@link CacheObject}.
     * @param log Logger.
     */
    public Dump(File dumpDir, boolean keepBinary, boolean raw, IgniteLogger log) {
        this(dumpDir, null, keepBinary, raw, log);
    }

    /**
     * @param dumpDir Dump directory.
     * @param consistentId If specified, read dump data only for specific node.
     * @param keepBinary If {@code true} then keep read entries in binary form.
     * @param raw If {@code true} then keep read entries in form of {@link KeyCacheObject} and {@link CacheObject}.
     * @param log Logger.
     */
    public Dump(File dumpDir, @Nullable String consistentId, boolean keepBinary, boolean raw, IgniteLogger log) {
        A.ensure(dumpDir != null, "dump directory is null");
        A.ensure(dumpDir.exists(), "dump directory not exists");

        this.dumpDir = dumpDir;
        this.consistentId = consistentId == null ? null : U.maskForFileName(consistentId);
        this.metadata = metadata(dumpDir, this.consistentId);
        this.keepBinary = keepBinary;
        this.cctx = standaloneKernalContext(dumpDir, log);
        this.raw = raw;
    }

    /**
     * @param dumpDir Dump directory.
     * @param log Logger.
     * @return Standalone kernal context.
     */
    private GridKernalContext standaloneKernalContext(File dumpDir, IgniteLogger log) {
        File binaryMeta = CacheObjectBinaryProcessorImpl.binaryWorkDir(dumpDir.getAbsolutePath(), F.first(metadata).folderName());
        File marshaller = new File(dumpDir, DFLT_MARSHALLER_PATH);

        A.ensure(binaryMeta.exists(), "binary metadata directory not exists");
        A.ensure(marshaller.exists(), "marshaller directory not exists");

        try {
            GridKernalContext kctx = new StandaloneGridKernalContext(log, binaryMeta, marshaller);

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

    /** @return List of node directories. */
    public List<String> nodesDirectories() {
        File[] dirs = new File(dumpDir, DFLT_STORE_DIR).listFiles(f -> f.isDirectory()
            && !(f.getAbsolutePath().endsWith(DFLT_BINARY_METADATA_PATH) || f.getAbsolutePath().endsWith(DFLT_MARSHALLER_PATH))
            && (consistentId == null || U.maskForFileName(f.getName()).contains(consistentId)));

        if (dirs == null)
            return Collections.emptyList();

        return Arrays.stream(dirs).map(File::getName).collect(Collectors.toList());
    }

    /** @return List of snapshot metadata saved in {@link #dumpDir}. */
    public List<SnapshotMetadata> metadata() throws IOException, IgniteCheckedException {
        return metadata;
    }

    /** @return List of snapshot metadata saved in {@link #dumpDir}. */
    private static List<SnapshotMetadata> metadata(File dumpDir, @Nullable String consistentId) {
        JdkMarshaller marsh = MarshallerUtils.jdkMarshaller("fake-node");

        ClassLoader clsLdr = U.resolveClassLoader(new IgniteConfiguration());

        File[] files = dumpDir.listFiles(f ->
            f.getName().endsWith(SNAPSHOT_METAFILE_EXT) && (consistentId == null || f.getName().startsWith(consistentId))
        );

        if (files == null)
            return Collections.emptyList();

        return Arrays.stream(files).map(meta -> {
            try (InputStream in = new BufferedInputStream(Files.newInputStream(meta.toPath()))) {
                return marsh.<SnapshotMetadata>unmarshal(in, clsLdr);
            }
            catch (IOException | IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }).filter(SnapshotMetadata::dump).collect(Collectors.toList());
    }

    /**
     * @param node Node directory name.
     * @param group Group id.
     * @return List of cache configs saved in dump for group.
     */
    public List<StoredCacheData> configs(String node, int group) {
        JdkMarshaller marsh = MarshallerUtils.jdkMarshaller(cctx.igniteInstanceName());

        return Arrays.stream(FilePageStoreManager.cacheDataFiles(dumpGroupDirectory(node, group))).map(f -> {
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
     * @param group Group id.
     * @return Dump iterator.
     */
    public List<Integer> partitions(String node, int group) {
        File[] parts = dumpGroupDirectory(node, group)
            .listFiles(f -> f.getName().startsWith(PART_FILE_PREFIX) && f.getName().endsWith(DUMP_FILE_EXT));

        if (parts == null)
            return Collections.emptyList();

        return Arrays.stream(parts)
            .map(partFile -> Integer.parseInt(partFile.getName().replace(PART_FILE_PREFIX, "").replace(DUMP_FILE_EXT, "")))
            .collect(Collectors.toList());
    }

    /**
     * @param node Node directory name.
     * @param group Group id.
     * @return Dump iterator.
     */
    public DumpedPartitionIterator iterator(String node, int group, int part) {
        FileIOFactory ioFactory = new RandomAccessFileIOFactory();

        FileIO dumpFile;

        try {
            dumpFile = ioFactory.create(new File(dumpGroupDirectory(node, group), PART_FILE_PREFIX + part + DUMP_FILE_EXT));
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        DumpEntrySerializer serializer = new DumpEntrySerializer(thLocBufs);

        serializer.kernalContext(cctx);
        serializer.keepBinary(keepBinary);
        serializer.raw(raw);

        return new DumpedPartitionIterator() {
            DumpEntry next;

            Set<Object> partKeys = new HashSet<>();

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
                    next = serializer.read(dumpFile, group, part);

                    /*
                     * During dumping entry can be dumped twice: by partition iterator and change listener.
                     * Excluding duplicates keys from iteration.
                     */
                    while (next != null && !partKeys.add(next.key()))
                        next = serializer.read(dumpFile, group, part);

                    if (next == null)
                        partKeys = null; // Let GC do the rest.
                }
                catch (IOException | IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }

            /** {@inheritDoc} */
            @Override public void close() {
                U.closeQuiet(dumpFile);

                partKeys = null;
            }
        };
    }

    /** @return Root dump directory. */
    public File dumpDirectory() {
        return dumpDir;
    }

    /** */
    private File dumpGroupDirectory(String node, int groupId) {
        File nodeDir = Paths.get(dumpDir.getAbsolutePath(), DFLT_STORE_DIR, node).toFile();

        assert nodeDir.exists() && nodeDir.isDirectory();

        File[] grpDirs = nodeDir.listFiles(f -> {
            if (!f.isDirectory()
                || (!f.getName().startsWith(CACHE_DIR_PREFIX)
                    && !f.getName().startsWith(CACHE_GRP_DIR_PREFIX)))
                return false;

            String grpName = f.getName().startsWith(CACHE_DIR_PREFIX)
                ? f.getName().replaceFirst(CACHE_DIR_PREFIX, "")
                : f.getName().replaceFirst(CACHE_GRP_DIR_PREFIX, "");

            return groupId == CU.cacheId(grpName);
        });

        if (grpDirs.length != 1)
            throw new IgniteException("Wrong number of group directories: " + grpDirs.length);

        return grpDirs[0];
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        closeAllComponents(cctx);
    }

    /**
     * Closeable dump iterator.
     */
    public interface DumpedPartitionIterator extends Iterator<DumpEntry>, AutoCloseable {
        // No-op.
    }
}
